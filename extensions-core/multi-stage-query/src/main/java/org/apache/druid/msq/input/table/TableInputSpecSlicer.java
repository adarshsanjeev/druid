/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.input.table;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.SlicerUtils;
import org.apache.druid.msq.querykit.DataSegmentTimelineView;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Slices {@link TableInputSpec} into {@link SegmentsInputSlice}.
 */
public class TableInputSpecSlicer implements InputSpecSlicer
{
  private final DataSegmentTimelineView timelineView;

  public TableInputSpecSlicer(DataSegmentTimelineView timelineView)
  {
    this.timelineView = timelineView;
  }

  @Override
  public boolean canSliceDynamic(InputSpec inputSpec)
  {
    return true;
  }

  @Override
  public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
  {
    final TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;

    final List<WeightedInputInstance> prunedPublishedSegments = new ArrayList<>();
    final List<DataSegmentWithInterval> prunedServedSegments = new ArrayList<>();

    for (DataSegmentWithInterval dataSegmentWithInterval : getPrunedSegmentSet(tableInputSpec)) {
      if (dataSegmentWithInterval.segment instanceof DataSegmentWithLocation) {
        prunedServedSegments.add(dataSegmentWithInterval);
      } else {
        prunedPublishedSegments.add(dataSegmentWithInterval);
      }
    }

    final List<WeightedInputInstance> groupedServedSegments = createWeightedSegmentSet(prunedServedSegments);

    final List<List<WeightedInputInstance>> assignments =
        SlicerUtils.makeSlicesStatic(
            Iterators.concat(groupedServedSegments.iterator(), prunedPublishedSegments.iterator()),
            WeightedInputInstance::getWeight,
            maxNumSlices
        );
    return makeSlices(tableInputSpec, assignments);
  }

  @Override
  public List<InputSlice> sliceDynamic(
      InputSpec inputSpec,
      int maxNumSlices,
      int maxFilesPerSlice,
      long maxBytesPerSlice
  )
  {
    final TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;

    final List<WeightedInputInstance> prunedSegments = new ArrayList<>();
    final List<DataSegmentWithInterval> prunedServedSegments = new ArrayList<>();

    for (DataSegmentWithInterval dataSegmentWithInterval : getPrunedSegmentSet(tableInputSpec)) {
      if (dataSegmentWithInterval.segment instanceof DataSegmentWithLocation) {
        prunedServedSegments.add(dataSegmentWithInterval);
      } else {
        prunedSegments.add(dataSegmentWithInterval);
      }
    }
    List<WeightedInputInstance> groupedServedSegments = createWeightedSegmentSet(prunedServedSegments);

    prunedSegments.addAll(groupedServedSegments);

    final List<List<WeightedInputInstance>> assignments =
        SlicerUtils.makeSlicesDynamic(
            prunedSegments.iterator(),
            WeightedInputInstance::getWeight,
            maxNumSlices,
            maxFilesPerSlice,
            maxBytesPerSlice
        );
    return makeSlices(tableInputSpec, assignments);
  }

  private Set<DataSegmentWithInterval> getPrunedSegmentSet(final TableInputSpec tableInputSpec)
  {
    final TimelineLookup<String, DataSegment> timeline =
        timelineView.getTimeline(tableInputSpec.getDataSource(), tableInputSpec.getIntervals()).orElse(null);

    if (timeline == null) {
      return Collections.emptySet();
    } else {
      final Iterator<DataSegmentWithInterval> dataSegmentIterator =
          tableInputSpec.getIntervals().stream()
                        .flatMap(interval -> timeline.lookup(interval).stream())
                        .flatMap(
                            holder ->
                                StreamSupport.stream(holder.getObject().spliterator(), false)
                                             .filter(chunk -> !chunk.getObject().isTombstone())
                                             .map(
                                                 chunk ->
                                                     new DataSegmentWithInterval(
                                                         chunk.getObject(),
                                                         holder.getInterval()
                                                     )
                                             )
                        ).iterator();

      return DimFilterUtils.filterShards(
          tableInputSpec.getFilter(),
          tableInputSpec.getFilterFields(),
          () -> dataSegmentIterator,
          segment -> segment.getSegment().getShardSpec(),
          new HashMap<>()
      );
    }
  }

  private static List<InputSlice> makeSlices(
      final TableInputSpec tableInputSpec,
      final List<List<WeightedInputInstance>> assignments
  )
  {
    final List<InputSlice> retVal = new ArrayList<>(assignments.size());

    for (final List<WeightedInputInstance> assignment : assignments) {

      final List<RichSegmentDescriptor> descriptors = new ArrayList<>();
      final List<DataServerRequestDescriptor> dataServerRequests = new ArrayList<>();

      for (final WeightedInputInstance weightedSegment : assignment) {
        if (weightedSegment instanceof DataSegmentWithInterval) {
          DataSegmentWithInterval dataSegmentWithInterval = (DataSegmentWithInterval) weightedSegment;
          descriptors.add(dataSegmentWithInterval.toRichSegmentDescriptor());
        } else {
          DataServerRequest serverRequest = (DataServerRequest) weightedSegment;
          dataServerRequests.add(serverRequest.toDataServerRequestDescriptor());
        }
      }

      if (descriptors.isEmpty() && dataServerRequests.isEmpty()) {
        retVal.add(NilInputSlice.INSTANCE);
      } else {
        retVal.add(new SegmentsInputSlice(tableInputSpec.getDataSource(), descriptors, dataServerRequests));
      }
    }

    return retVal;
  }

  /**
   * Creates a list of {@link WeightedInputInstance} from the prunedServedSegments parameter.
   * The function selects a data server from the servers hosting the segment, and then groups segments on the basis of
   * data servers.
   * The returned value is a list of {@link WeightedInputInstance}, each of which denotes either a {@link DataSegmentWithInterval},
   * in the case of a segment or a {@link DataServerRequest} for a request to a data server. A data server request fetches
   * the results of all relevent segments from the data server.
   */
  private static List<WeightedInputInstance> createWeightedSegmentSet(List<DataSegmentWithInterval> prunedServedSegments)
  {
    // Create a map of server to segment for loaded segments.
    final Map<DruidServerMetadata, Set<DataSegmentWithInterval>> serverVsSegmentsMap = new HashMap<>();
    for (DataSegmentWithInterval dataSegmentWithInterval : prunedServedSegments) {
      DataSegmentWithLocation segmentWithLocation = (DataSegmentWithLocation) dataSegmentWithInterval.segment;
      // Choose a server out of the ones available.
      DruidServerMetadata druidServerMetadata = DataServerSelector.RANDOM.getSelectServerFunction().apply(segmentWithLocation.getServers());

      serverVsSegmentsMap.computeIfAbsent(druidServerMetadata, ignored -> new HashSet<>());
      serverVsSegmentsMap.get(druidServerMetadata).add(dataSegmentWithInterval);
    }

    List<WeightedInputInstance> retVal = new ArrayList<>();
    for (Map.Entry<DruidServerMetadata, Set<DataSegmentWithInterval>> druidServerMetadataSetEntry : serverVsSegmentsMap.entrySet()) {
      DataServerRequest dataServerRequest = new DataServerRequest(
          druidServerMetadataSetEntry.getKey(),
          ImmutableList.copyOf(druidServerMetadataSetEntry.getValue())
      );
      retVal.add(dataServerRequest);
    }

    return retVal;
  }

  private interface WeightedInputInstance
  {
    long getWeight();
  }

  private static class DataSegmentWithInterval implements WeightedInputInstance
  {
    private final DataSegment segment;
    private final Interval interval;

    public DataSegmentWithInterval(DataSegment segment, Interval interval)
    {
      this.segment = Preconditions.checkNotNull(segment, "segment");
      this.interval = Preconditions.checkNotNull(interval, "interval");
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public RichSegmentDescriptor toRichSegmentDescriptor()
    {
      return new RichSegmentDescriptor(
          segment.getInterval(),
          interval,
          segment.getVersion(),
          segment.getShardSpec().getPartitionNum()
      );
    }

    @Override
    public long getWeight()
    {
      return segment.getSize();
    }
  }

  private static class DataServerRequest implements WeightedInputInstance
  {
    private static final long DATA_SERVER_WEIGHT_ESTIMATION = 5000L;
    private final List<DataSegmentWithInterval> segments;
    private final DruidServerMetadata serverMetadata;

    public DataServerRequest(DruidServerMetadata serverMetadata, List<DataSegmentWithInterval> segments)
    {
      this.segments = Preconditions.checkNotNull(segments, "segments");
      this.serverMetadata = Preconditions.checkNotNull(serverMetadata, "server");
    }

    @Override
    public long getWeight()
    {
      return segments.size() * DATA_SERVER_WEIGHT_ESTIMATION;
    }

    public DataServerRequestDescriptor toDataServerRequestDescriptor()
    {
      return new DataServerRequestDescriptor(
          serverMetadata,
          segments.stream().map(DataSegmentWithInterval::toRichSegmentDescriptor).collect(Collectors.toList()));
    }
  }
}
