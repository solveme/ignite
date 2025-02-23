/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.stat.task;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.H2Row;
import org.apache.ignite.internal.processors.query.stat.ColumnStatistics;
import org.apache.ignite.internal.processors.query.stat.ColumnStatisticsCollector;
import org.apache.ignite.internal.processors.query.stat.GatherStatisticCancelException;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsHelper;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsRepository;
import org.apache.ignite.internal.processors.query.stat.LocalStatisticsGatheringContext;
import org.apache.ignite.internal.processors.query.stat.ObjectPartitionStatisticsImpl;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsColumnConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.h2.table.Column;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 * Implementation of statistic collector. Load existing, gather or remove some columns statistics and save it back to
 * repository.
 *
 * In case of context.force - recollect all columns.
 * In case of context.configuration contains less columns than existing statistics - remove some columns.
 * In case of existing statistics contains column with required version - leave it as is.
 *
 * Reset obsolescence if needed.
 */
public class GatherPartitionStatistics implements Callable<ObjectPartitionStatisticsImpl> {
    /** Check "Canceled" flag each processed row. */
    private static final int CANCELLED_CHECK_INTERVAL = 100;

    /** Statistics repository. */
    private final IgniteStatisticsRepository statRepo;

    /** Partition id. */
    private final int partId;

    /** Gathering context. */
    private final LocalStatisticsGatheringContext gathCtx;

    /** Ignite logger. */
    private final IgniteLogger log;

    /** Collection time. */
    private long time;

    /**
     * Constructor.
     *
     * @param statRepo Statistics repository.
     * @param gathCtx Gathering context.
     * @param partId Target partition id in context.
     * @param log Logger.
     */
    public GatherPartitionStatistics(
        IgniteStatisticsRepository statRepo,
        LocalStatisticsGatheringContext gathCtx,
        int partId,
        IgniteLogger log
    ) {
        this.statRepo = statRepo;
        this.partId = partId;
        this.gathCtx = gathCtx;
        this.log = log;
    }

    /**
     * @return Partition id.
     */
    public int partition() {
        return partId;
    }

    /**
     * @return LocalStatisticsGatheringContext.
     */
    public LocalStatisticsGatheringContext context() {
        return gathCtx;
    }

    /**
     * Reuse or gather new partition statistics according to context and repository state.
     * Save partition statistcs and obsolescence info back to repository if needed.
     *
     * @return Partition statistics.
     */
    @Override public ObjectPartitionStatisticsImpl call() {
        time = U.currentTimeMillis();

        if (gathCtx.cancelled())
            throw new GatherStatisticCancelException();

        GridCacheContext<?, ?> cctx = gathCtx.table().cacheContext();

        if (cctx == null || !(cctx.gate().enterIfNotStopped()))
            throw new GatherStatisticCancelException();

        try {
            return processPartition(cctx);
        }
        finally {
            cctx.gate().leave();
        }
    }

    /**
     * Decide what column should be gathered and what partition statistics already has and either fix it or
     * collect new data.
     *
     * @param cctx Cache context to get partition from.
     * @return New partition statistics.
     */
    private ObjectPartitionStatisticsImpl processPartition(
        GridCacheContext<?, ?> cctx
    ) {
        ObjectPartitionStatisticsImpl partStat = statRepo.getLocalPartitionStatistics(
            gathCtx.configuration().key(), partId);

        Map<String, StatisticsColumnConfiguration> colsToCollect = getColumnsToCollect(partStat);
        Set<String> colsToRemove = getColumnsToRemove(partStat);

        // Try to use existing statitsics.
        if (F.isEmpty(colsToCollect))
            return fixExisting(partStat, colsToRemove);
        else
            return recollectPartition(cctx, partStat, colsToCollect, colsToRemove);
    }

    /**
     * Fix existing partition statistics, update repo and return resulting partition statistics.
     *
     * @param partStat Partition statistics to fix.
     * @param colsToRemove Columns to remove.
     * @return New "fixed" partition statistics or existing, if colsToRemove is empty.
     */
    private ObjectPartitionStatisticsImpl fixExisting(ObjectPartitionStatisticsImpl partStat, Set<String> colsToRemove) {
        if (log.isDebugEnabled())
            log.debug("Existing parititon statistics fit to configuration requirements. " +
                "Skipping recollection for " + gathCtx.configuration().key() + "[" + partId + "].");

        ObjectPartitionStatisticsImpl res;

        if (F.isEmpty(colsToRemove))
            // No changes - no need to write existing parition back.
            res = partStat;
        else {
            Map<String, ColumnStatistics> allCols = new HashMap<>(partStat.columnsStatistics());

            for (String col : colsToRemove)
                allCols.remove(col);

            res = new ObjectPartitionStatisticsImpl(partStat.partId(), getRowCount(allCols), partStat.updCnt(),
                allCols);

            assert !allCols.isEmpty() : "No columns left after fixing existing partition statistics.";

            statRepo.replaceLocalPartitionStatistics(gathCtx.configuration().key(), res);
        }

        return res;
    }

    /**
     * Collect some statistics, fix existing in repo and return resulting partition statistics.
     *
     * @param cctx Cache context to get partition from.
     * @param partStat Existing partition statistics to fix or use as a base.
     * @param colsToCollect Columns to collect.
     * @param colsToRemove Columns to remove.
     * @return New partition statistics.
     */
    private ObjectPartitionStatisticsImpl recollectPartition(
        GridCacheContext<?, ?> cctx,
        ObjectPartitionStatisticsImpl partStat,
        Map<String, StatisticsColumnConfiguration> colsToCollect,
        Set<String> colsToRemove
    ) {
        CacheGroupContext grp = cctx.group();
        GridDhtPartitionTopology top = grp.topology();
        AffinityTopologyVersion topVer = top.readyTopologyVersion();

        GridDhtLocalPartition locPart = top.localPartition(partId, topVer, false);

        if (locPart == null)
            throw new GatherStatisticCancelException();

        boolean reserved = locPart.reserve();

        GridH2Table tbl = gathCtx.table();

        ObjectPartitionStatisticsImpl res = null;

        try {
            if (!reserved || (locPart.state() != OWNING)) {
                if (log.isDebugEnabled()) {
                    log.debug("Partition not owning. Need to retry [part=" + partId +
                        ", tbl=" + tbl.identifier() + ']');
                }

                throw new GatherStatisticCancelException();
            }

            Column cols[] = IgniteStatisticsHelper.filterColumns(tbl.getColumns(), colsToCollect.keySet());

            ColumnStatisticsCollector[] collectors = new ColumnStatisticsCollector[cols.length];

            for (int i = 0; i < cols.length; ++i) {
                long colCfgVer = colsToCollect.get(cols[i].getName()).version();

                collectors[i] = new ColumnStatisticsCollector(cols[i], tbl::compareTypeSafe, colCfgVer);
            }

            GridH2RowDescriptor rowDesc = tbl.rowDescriptor();
            GridQueryTypeDescriptor typeDesc = rowDesc.type();

            try {
                int checkInt = CANCELLED_CHECK_INTERVAL;

                if (log.isDebugEnabled()) {
                    log.debug("Start partition scan [part=" + partId +
                        ", tbl=" + gathCtx.table().identifier() + ']');
                }

                for (CacheDataRow row : grp.offheap().cachePartitionIterator(
                    gathCtx.table().cacheId(), partId, null, false)
                ) {
                    if (--checkInt == 0) {
                        if (gathCtx.future().isCancelled())
                            throw new GatherStatisticCancelException();

                        checkInt = CANCELLED_CHECK_INTERVAL;
                    }

                    if (!typeDesc.matchType(row.value()) || wasExpired(row))
                        continue;

                    H2Row h2row = new H2CacheRow(rowDesc, row);

                    for (ColumnStatisticsCollector colStat : collectors)
                        colStat.add(h2row.getValue(colStat.col().getColumnId()));
                }
            }
            catch (IgniteCheckedException e) {
                log.warning(String.format("Unable to collect partition level statistics by %s.%s:%d due to %s",
                    tbl.identifier().schema(), tbl.identifier().table(), partId, e.getMessage()));

                throw new IgniteException("Unable to collect partition level statistics", e);
            }

            Map<String, ColumnStatistics> colStats = Arrays.stream(collectors).collect(
                Collectors.toMap(csc -> csc.col().getName(), ColumnStatisticsCollector::finish));

            // Add existing to full replace existing statistics with new one.
            if (partStat != null) {
                for (Map.Entry<String, ColumnStatistics> oldColStat : partStat.columnsStatistics().entrySet()) {
                    if (!colsToRemove.contains(oldColStat.getKey()))
                        colStats.putIfAbsent(oldColStat.getKey(), oldColStat.getValue());
                }
            }

            res = new ObjectPartitionStatisticsImpl(
                partId,
                getRowCount(colStats),
                locPart.updateCounter(),
                colStats
            );

        }
        finally {
            if (reserved)
                locPart.release();
        }

        statRepo.replaceLocalPartitionStatistics(gathCtx.configuration().key(), res);

        if (gathCtx.configuration().columns().size() == colsToCollect.size())
            statRepo.refreshObsolescence(gathCtx.configuration().key(), partId);

        return res;
    }

    /**
     * Row count should be calculated as max(total) of existing columns.
     *
     * @param cols All columns map.
     * @return Total row count.
     */
    private long getRowCount(Map<String, ColumnStatistics> cols) {
        long res = 0L;

        for (ColumnStatistics colStat : cols.values()) {
            if (res < colStat.total())
                res = colStat.total();
        }

        return res;
    }

    /**
     * Get columns list to collect statistics by.
     */
    private Map<String, StatisticsColumnConfiguration> getColumnsToCollect(
        ObjectPartitionStatisticsImpl partStat
    ) {
        if (partStat == null || gathCtx.forceRecollect())
            return gathCtx.configuration().columns();

        Map<String, StatisticsColumnConfiguration> res = new HashMap<>();

        for (StatisticsColumnConfiguration colStatCfg : gathCtx.configuration().columns().values()) {
            ColumnStatistics colStat = partStat.columnStatistics(colStatCfg.name());

            if (colStat == null || colStatCfg.version() > colStat.version())
                res.put(colStatCfg.name(), colStatCfg);
        }

        return res;
    }

    /**
     * Get columns list to remove statistics by.
     */
    private Set<String> getColumnsToRemove(@Nullable ObjectPartitionStatisticsImpl partStat) {
        if (partStat == null)
            return Collections.emptySet();

        Set<String> res = new HashSet<>();
        Map<String, StatisticsColumnConfiguration> colCfg = gathCtx.configuration().columns();

        for (String col : partStat.columnsStatistics().keySet()) {
            if (!colCfg.containsKey(col))
                res.add(col);
        }

        return res;
    }

    /**
     * Test if row expired.
     *
     * @param row Row to test.
     * @return {@code true} if row expired, {@code false} - otherwise.
     */
    private boolean wasExpired(CacheDataRow row) {
        return row.expireTime() > 0 && row.expireTime() <= time;
    }
}
