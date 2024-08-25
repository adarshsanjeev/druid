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

package org.apache.druid.msq.exec;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.FrameWithPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.processor.BlockingQueueOutputChannelFactory;
import org.apache.druid.frame.processor.Bouncer;
import org.apache.druid.frame.processor.ComposingOutputChannelFactory;
import org.apache.druid.frame.processor.FileOutputChannelFactory;
import org.apache.druid.frame.processor.FrameChannelHashPartitioner;
import org.apache.druid.frame.processor.FrameChannelMixer;
import org.apache.druid.frame.processor.FrameProcessor;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.processor.OutputChannel;
import org.apache.druid.frame.processor.OutputChannelFactory;
import org.apache.druid.frame.processor.OutputChannels;
import org.apache.druid.frame.processor.PartitionedOutputChannel;
import org.apache.druid.frame.processor.SuperSorter;
import org.apache.druid.frame.processor.SuperSorterProgressTracker;
import org.apache.druid.frame.processor.manager.ProcessorManager;
import org.apache.druid.frame.processor.manager.ProcessorManagers;
import org.apache.druid.frame.util.DurableStorageUtils;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.counters.CounterNames;
import org.apache.druid.msq.counters.CounterTracker;
import org.apache.druid.msq.indexing.CountingOutputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelFactory;
import org.apache.druid.msq.indexing.InputChannelsImpl;
import org.apache.druid.msq.indexing.processor.KeyStatisticsCollectionProcessor;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSliceReader;
import org.apache.druid.msq.input.InputSlices;
import org.apache.druid.msq.input.MapInputSliceReader;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.NilInputSliceReader;
import org.apache.druid.msq.input.external.ExternalInputSlice;
import org.apache.druid.msq.input.external.ExternalInputSliceReader;
import org.apache.druid.msq.input.inline.InlineInputSlice;
import org.apache.druid.msq.input.inline.InlineInputSliceReader;
import org.apache.druid.msq.input.lookup.LookupInputSlice;
import org.apache.druid.msq.input.lookup.LookupInputSliceReader;
import org.apache.druid.msq.input.stage.InputChannels;
import org.apache.druid.msq.input.stage.StageInputSlice;
import org.apache.druid.msq.input.stage.StageInputSliceReader;
import org.apache.druid.msq.input.table.SegmentsInputSlice;
import org.apache.druid.msq.input.table.SegmentsInputSliceReader;
import org.apache.druid.msq.kernel.FrameContext;
import org.apache.druid.msq.kernel.FrameProcessorFactory;
import org.apache.druid.msq.kernel.ProcessorsAndChannels;
import org.apache.druid.msq.kernel.ShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.shuffle.output.DurableStorageOutputChannelFactory;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.utils.CloseableUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Main worker logic for executing a {@link WorkOrder} in a {@link FrameProcessorExecutor}.
 */
public class RunWorkOrder
{
  private final WorkOrder workOrder;
  private final InputChannelFactory inputChannelFactory;
  private final CounterTracker counterTracker;
  private final FrameProcessorExecutor exec;
  private final String cancellationId;
  private final int parallelism;
  private final WorkerContext workerContext;
  private final FrameContext frameContext;
  private final RunWorkOrderListener listener;
  private final boolean reindex;
  private final boolean removeNullBytes;
  private final ByteTracker intermediateSuperSorterLocalStorageTracker;
  private final AtomicBoolean started = new AtomicBoolean();

  @MonotonicNonNull
  private InputSliceReader inputSliceReader;
  @MonotonicNonNull
  private OutputChannelFactory workOutputChannelFactory;
  @MonotonicNonNull
  private OutputChannelFactory shuffleOutputChannelFactory;
  @MonotonicNonNull
  private ResultAndChannels<?> workResultAndOutputChannels;
  @MonotonicNonNull
  private SettableFuture<ClusterByPartitions> stagePartitionBoundariesFuture;
  @MonotonicNonNull
  private ListenableFuture<OutputChannels> stageOutputChannelsFuture;

  public RunWorkOrder(
      final WorkOrder workOrder,
      final InputChannelFactory inputChannelFactory,
      final CounterTracker counterTracker,
      final FrameProcessorExecutor exec,
      final String cancellationId,
      final WorkerContext workerContext,
      final FrameContext frameContext,
      final RunWorkOrderListener listener,
      final boolean reindex,
      final boolean removeNullBytes
  )
  {
    this.workOrder = workOrder;
    this.inputChannelFactory = inputChannelFactory;
    this.counterTracker = counterTracker;
    this.exec = exec;
    this.cancellationId = cancellationId;
    this.parallelism = workerContext.threadCount();
    this.workerContext = workerContext;
    this.frameContext = frameContext;
    this.listener = listener;
    this.reindex = reindex;
    this.removeNullBytes = removeNullBytes;
    this.intermediateSuperSorterLocalStorageTracker =
        new ByteTracker(
            frameContext.storageParameters().isIntermediateStorageLimitConfigured()
            ? frameContext.storageParameters().getIntermediateSuperSorterStorageMaxLocalBytes()
            : Long.MAX_VALUE
        );
  }

  /**
   * Start execution of the provided {@link WorkOrder} in the provided {@link FrameProcessorExecutor}.
   *
   * Execution proceeds asynchronously after this method returns. The {@link RunWorkOrderListener} passed to the
   * constructor of this instance can be used to track progress.
   */
  public void start() throws IOException
  {
    if (started.getAndSet(true)) {
      throw new ISE("Already started");
    }

    final StageDefinition stageDef = workOrder.getStageDefinition();

    try {
      makeInputSliceReader();
      makeWorkOutputChannelFactory();
      makeShuffleOutputChannelFactory();
      makeAndRunWorkProcessors();

      if (stageDef.doesShuffle()) {
        makeAndRunShuffleProcessors();
      } else {
        // No shuffling: work output _is_ stage output. Retain read-only versions to reduce memory footprint.
        stageOutputChannelsFuture =
            Futures.immediateFuture(workResultAndOutputChannels.getOutputChannels().readOnly());
      }

      setUpCompletionCallbacks();
    }
    catch (Throwable t) {
      // If start() has problems, cancel anything that was already kicked off, and close the FrameContext.
      try {
        exec.cancel(cancellationId);
      }
      catch (Throwable t2) {
        t.addSuppressed(t2);
      }

      CloseableUtils.closeAndSuppressExceptions(frameContext, t::addSuppressed);
      throw t;
    }
  }

  /**
   * Settable {@link ClusterByPartitions} future for global sort. Necessary because we don't know ahead of time
   * what the boundaries will be. The controller decides based on statistics from all workers. Once the controller
   * decides, its decision is written to this future, which allows sorting on workers to proceed.
   */
  @Nullable
  public SettableFuture<ClusterByPartitions> getStagePartitionBoundariesFuture()
  {
    return stagePartitionBoundariesFuture;
  }

  private void makeInputSliceReader()
  {
    if (inputSliceReader != null) {
      throw new ISE("inputSliceReader already created");
    }

    final String queryId = workOrder.getQueryDefinition().getQueryId();

    final InputChannels inputChannels =
        new InputChannelsImpl(
            workOrder.getQueryDefinition(),
            InputSlices.allReadablePartitions(workOrder.getInputs()),
            inputChannelFactory,
            () -> ArenaMemoryAllocator.createOnHeap(frameContext.memoryParameters().getStandardFrameSize()),
            exec,
            cancellationId,
            removeNullBytes
        );

    inputSliceReader = new MapInputSliceReader(
        ImmutableMap.<Class<? extends InputSlice>, InputSliceReader>builder()
                    .put(NilInputSlice.class, NilInputSliceReader.INSTANCE)
                    .put(StageInputSlice.class, new StageInputSliceReader(queryId, inputChannels))
                    .put(ExternalInputSlice.class, new ExternalInputSliceReader(frameContext.tempDir("external")))
                    .put(InlineInputSlice.class, new InlineInputSliceReader(frameContext.segmentWrangler()))
                    .put(LookupInputSlice.class, new LookupInputSliceReader(frameContext.segmentWrangler()))
                    .put(SegmentsInputSlice.class, new SegmentsInputSliceReader(frameContext, reindex))
                    .build()
    );
  }

  private void makeWorkOutputChannelFactory()
  {
    if (workOutputChannelFactory != null) {
      throw new ISE("processorOutputChannelFactory already created");
    }

    final OutputChannelFactory baseOutputChannelFactory;

    if (workOrder.getStageDefinition().doesShuffle()) {
      // Writing to a consumer in the same JVM (which will be set up later on in this method). Use the large frame
      // size if we're writing to a SuperSorter, since we'll generate fewer temp files if we use larger frames.
      // Otherwise, use the standard frame size.
      final int frameSize;

      if (workOrder.getStageDefinition().getShuffleSpec().kind().isSort()) {
        frameSize = frameContext.memoryParameters().getLargeFrameSize();
      } else {
        frameSize = frameContext.memoryParameters().getStandardFrameSize();
      }

      baseOutputChannelFactory = new BlockingQueueOutputChannelFactory(frameSize);
    } else {
      // Writing stage output.
      baseOutputChannelFactory = makeStageOutputChannelFactory();
    }

    workOutputChannelFactory = new CountingOutputChannelFactory(
        baseOutputChannelFactory,
        counterTracker.channel(CounterNames.outputChannel())
    );
  }

  private void makeShuffleOutputChannelFactory()
  {
    shuffleOutputChannelFactory =
        new CountingOutputChannelFactory(
            makeStageOutputChannelFactory(),
            counterTracker.channel(CounterNames.shuffleChannel())
        );
  }

  /**
   * Use {@link FrameProcessorFactory#makeProcessors} to create {@link ProcessorsAndChannels}. Executes the
   * processors using {@link #exec} and sets the output channels in {@link #workResultAndOutputChannels}.
   *
   * @param <FactoryType>         type of {@link StageDefinition#getProcessorFactory()}
   * @param <ProcessorReturnType> return type of {@link FrameProcessor} created by the manager
   * @param <ManagerReturnType>   result type of {@link ProcessorManager#result()}
   * @param <ExtraInfoType>       type of {@link WorkOrder#getExtraInfo()}
   */
  private <FactoryType extends FrameProcessorFactory<ProcessorReturnType, ManagerReturnType, ExtraInfoType>, ProcessorReturnType, ManagerReturnType, ExtraInfoType> void makeAndRunWorkProcessors()
      throws IOException
  {
    if (workResultAndOutputChannels != null) {
      throw new ISE("workResultAndOutputChannels already set");
    }

    @SuppressWarnings("unchecked")
    final FactoryType processorFactory = (FactoryType) workOrder.getStageDefinition().getProcessorFactory();

    @SuppressWarnings("unchecked")
    final ProcessorsAndChannels<ProcessorReturnType, ManagerReturnType> processors =
        processorFactory.makeProcessors(
            workOrder.getStageDefinition(),
            workOrder.getWorkerNumber(),
            workOrder.getInputs(),
            inputSliceReader,
            (ExtraInfoType) workOrder.getExtraInfo(),
            workOutputChannelFactory,
            frameContext,
            parallelism,
            counterTracker,
            listener::onWarning,
            removeNullBytes
        );

    final ProcessorManager<ProcessorReturnType, ManagerReturnType> processorManager = processors.getProcessorManager();

    final int maxOutstandingProcessors;

    if (processors.getOutputChannels().getAllChannels().isEmpty()) {
      // No output channels: run up to "parallelism" processors at once.
      maxOutstandingProcessors = Math.max(1, parallelism);
    } else {
      // If there are output channels, that acts as a ceiling on the number of processors that can run at once.
      maxOutstandingProcessors =
          Math.max(1, Math.min(parallelism, processors.getOutputChannels().getAllChannels().size()));
    }

    final ListenableFuture<ManagerReturnType> workResultFuture = exec.runAllFully(
        processorManager,
        maxOutstandingProcessors,
        frameContext.processorBouncer(),
        cancellationId
    );

    workResultAndOutputChannels = new ResultAndChannels<>(workResultFuture, processors.getOutputChannels());
  }

  private void makeAndRunShuffleProcessors()
  {
    if (stageOutputChannelsFuture != null) {
      throw new ISE("stageOutputChannelsFuture already set");
    }

    final ShuffleSpec shuffleSpec = workOrder.getStageDefinition().getShuffleSpec();

    final ShufflePipelineBuilder shufflePipeline = new ShufflePipelineBuilder(
        workOrder,
        counterTracker,
        exec,
        cancellationId,
        frameContext
    );

    shufflePipeline.initialize(workResultAndOutputChannels);
    shufflePipeline.gatherResultKeyStatisticsAndReportDoneReadingInputIfNeeded();

    switch (shuffleSpec.kind()) {
      case MIX:
        shufflePipeline.mix(shuffleOutputChannelFactory);
        break;

      case HASH:
        shufflePipeline.hashPartition(shuffleOutputChannelFactory);
        break;

      case HASH_LOCAL_SORT:
        final OutputChannelFactory hashOutputChannelFactory;

        if (shuffleSpec.partitionCount() == 1) {
          // Single partition; no need to write temporary files.
          hashOutputChannelFactory =
              new BlockingQueueOutputChannelFactory(frameContext.memoryParameters().getStandardFrameSize());
        } else {
          // Multi-partition; write temporary files and then sort each one file-by-file.
          hashOutputChannelFactory =
              new FileOutputChannelFactory(
                  frameContext.tempDir("hash-parts"),
                  frameContext.memoryParameters().getStandardFrameSize(),
                  null
              );
        }

        shufflePipeline.hashPartition(hashOutputChannelFactory);
        shufflePipeline.localSort(shuffleOutputChannelFactory);
        break;

      case GLOBAL_SORT:
        shufflePipeline.globalSort(shuffleOutputChannelFactory, makeGlobalSortPartitionBoundariesFuture());
        break;

      default:
        throw new UOE("Cannot handle shuffle kind [%s]", shuffleSpec.kind());
    }

    stageOutputChannelsFuture = shufflePipeline.build();
  }

  private ListenableFuture<ClusterByPartitions> makeGlobalSortPartitionBoundariesFuture()
  {
    if (workOrder.getStageDefinition().mustGatherResultKeyStatistics()) {
      if (stagePartitionBoundariesFuture != null) {
        throw new ISE("Cannot call 'makeGlobalSortPartitionBoundariesFuture' twice");
      }

      return (stagePartitionBoundariesFuture = SettableFuture.create());
    } else {
      // Result key stats aren't needed, so the partition boundaries are knowable ahead of time. Compute them now.
      final ClusterByPartitions boundaries =
          workOrder.getStageDefinition()
                   .generatePartitionBoundariesForShuffle(null)
                   .valueOrThrow();

      return Futures.immediateFuture(boundaries);
    }
  }

  private void setUpCompletionCallbacks()
  {
    Futures.addCallback(
        Futures.allAsList(
            Arrays.asList(
                workResultAndOutputChannels.getResultFuture(),
                stageOutputChannelsFuture
            )
        ),
        new FutureCallback<List<Object>>()
        {
          @Override
          public void onSuccess(final List<Object> workerResultAndOutputChannelsResolved)
          {
            final Object resultObject = workerResultAndOutputChannelsResolved.get(0);
            final OutputChannels outputChannels = (OutputChannels) workerResultAndOutputChannelsResolved.get(1);

            if (workOrder.getOutputChannelMode() != OutputChannelMode.MEMORY) {
              // In non-MEMORY output channel modes, call onOutputChannelAvailable when all work is done.
              // (In MEMORY mode, we would have called onOutputChannelAvailable when the channels were created.)
              for (final OutputChannel channel : outputChannels.getAllChannels()) {
                listener.onOutputChannelAvailable(channel);
              }
            }

            if (workOrder.getOutputChannelMode().isDurable()) {
              // In DURABLE_STORAGE output channel mode, write a success file once all work is done.
              writeDurableStorageSuccessFile();
            }

            listener.onSuccess(resultObject);
          }

          @Override
          public void onFailure(final Throwable t)
          {
            listener.onFailure(t);
          }
        },
        Execs.directExecutor()
    );
  }

  /**
   * Write {@link DurableStorageUtils#SUCCESS_MARKER_FILENAME} for a particular stage, if durable storage is enabled.
   */
  private void writeDurableStorageSuccessFile()
  {
    final DurableStorageOutputChannelFactory durableStorageOutputChannelFactory =
        makeDurableStorageOutputChannelFactory(
            frameContext.tempDir("durable"),
            frameContext.memoryParameters().getStandardFrameSize(),
            workOrder.getOutputChannelMode() == OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS
        );

    try {
      durableStorageOutputChannelFactory.createSuccessFile(workerContext.workerId());
    }
    catch (IOException e) {
      throw new ISE(
          e,
          "Unable to create success file at location[%s]",
          durableStorageOutputChannelFactory.getSuccessFilePath()
      );
    }
  }

  private OutputChannelFactory makeStageOutputChannelFactory()
  {
    // Use the standard frame size, since we assume this size when computing how much is needed to merge output
    // files from different workers.
    final int frameSize = frameContext.memoryParameters().getStandardFrameSize();
    final OutputChannelMode outputChannelMode = workOrder.getOutputChannelMode();

    switch (outputChannelMode) {
      case MEMORY:
        // Use ListeningOutputChannelFactory to capture output channels as they are created, rather than when
        // work is complete.
        return new ListeningOutputChannelFactory(
            new BlockingQueueOutputChannelFactory(frameSize),
            listener::onOutputChannelAvailable
        );

      case LOCAL_STORAGE:
        final File fileChannelDirectory =
            frameContext.tempDir(StringUtils.format("output_stage_%06d", workOrder.getStageNumber()));
        return new FileOutputChannelFactory(fileChannelDirectory, frameSize, null);

      case DURABLE_STORAGE_INTERMEDIATE:
      case DURABLE_STORAGE_QUERY_RESULTS:
        return makeDurableStorageOutputChannelFactory(
            frameContext.tempDir("durable"),
            frameSize,
            outputChannelMode == OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS
        );

      default:
        throw DruidException.defensive("No handling for outputChannelMode[%s]", outputChannelMode);
    }
  }

  private OutputChannelFactory makeSuperSorterIntermediateOutputChannelFactory(final File tmpDir)
  {
    final int frameSize = frameContext.memoryParameters().getLargeFrameSize();
    final File fileChannelDirectory =
        new File(tmpDir, StringUtils.format("intermediate_output_stage_%06d", workOrder.getStageNumber()));
    final FileOutputChannelFactory fileOutputChannelFactory =
        new FileOutputChannelFactory(fileChannelDirectory, frameSize, intermediateSuperSorterLocalStorageTracker);

    if (workOrder.getOutputChannelMode().isDurable()
        && frameContext.storageParameters().isIntermediateStorageLimitConfigured()) {
      final boolean isQueryResults =
          workOrder.getOutputChannelMode() == OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS;
      return new ComposingOutputChannelFactory(
          ImmutableList.of(
              fileOutputChannelFactory,
              makeDurableStorageOutputChannelFactory(tmpDir, frameSize, isQueryResults)
          ),
          frameSize
      );
    } else {
      return fileOutputChannelFactory;
    }
  }

  private DurableStorageOutputChannelFactory makeDurableStorageOutputChannelFactory(
      final File tmpDir,
      final int frameSize,
      final boolean isQueryResults
  )
  {
    return DurableStorageOutputChannelFactory.createStandardImplementation(
        workOrder.getQueryDefinition().getQueryId(),
        workOrder.getWorkerNumber(),
        workOrder.getStageNumber(),
        workerContext.workerId(),
        frameSize,
        MSQTasks.makeStorageConnector(workerContext.injector(), workerContext.tempDir()),
        tmpDir,
        isQueryResults
    );
  }

  /**
   * Helper for {@link RunWorkOrder#makeAndRunShuffleProcessors()}. Builds a {@link FrameProcessor} pipeline to
   * handle the shuffle.
   */
  private class ShufflePipelineBuilder
  {
    private final WorkOrder workOrder;
    private final CounterTracker counterTracker;
    private final FrameProcessorExecutor exec;
    private final String cancellationId;
    private final FrameContext frameContext;

    // Current state of the pipeline. It's a future to allow pipeline construction to be deferred if necessary.
    private ListenableFuture<ResultAndChannels<?>> pipelineFuture;

    public ShufflePipelineBuilder(
        final WorkOrder workOrder,
        final CounterTracker counterTracker,
        final FrameProcessorExecutor exec,
        final String cancellationId,
        final FrameContext frameContext
    )
    {
      this.workOrder = workOrder;
      this.counterTracker = counterTracker;
      this.exec = exec;
      this.cancellationId = cancellationId;
      this.frameContext = frameContext;
    }

    /**
     * Start the pipeline with the outputs of the main processor.
     */
    public void initialize(final ResultAndChannels<?> resultAndChannels)
    {
      if (pipelineFuture != null) {
        throw new ISE("already initialized");
      }

      pipelineFuture = Futures.immediateFuture(resultAndChannels);
    }

    /**
     * Add {@link FrameChannelMixer}, which mixes all current outputs into a single channel from the provided factory.
     */
    public void mix(final OutputChannelFactory outputChannelFactory)
    {
      // No sorting or statistics gathering, just combining all outputs into one big partition. Use a mixer to get
      // everything into one file. Note: even if there is only one output channel, we'll run it through the mixer
      // anyway, to ensure the data gets written to a file. (httpGetChannelData requires files.)

      push(
          resultAndChannels -> {
            final OutputChannel outputChannel = outputChannelFactory.openChannel(0);

            final FrameChannelMixer mixer =
                new FrameChannelMixer(
                    resultAndChannels.getOutputChannels().getAllReadableChannels(),
                    outputChannel.getWritableChannel()
                );

            return new ResultAndChannels<>(
                exec.runFully(mixer, cancellationId),
                OutputChannels.wrap(Collections.singletonList(outputChannel.readOnly()))
            );
          }
      );
    }

    /**
     * Add {@link KeyStatisticsCollectionProcessor} if {@link StageDefinition#mustGatherResultKeyStatistics()}.
     *
     * Calls {@link RunWorkOrderListener#onDoneReadingInput(ClusterByStatisticsSnapshot)} when statistics are gathered.
     * If statistics were not needed, calls the listener immediately.
     */
    public void gatherResultKeyStatisticsAndReportDoneReadingInputIfNeeded()
    {
      push(
          resultAndChannels -> {
            final StageDefinition stageDefinition = workOrder.getStageDefinition();
            final OutputChannels channels = resultAndChannels.getOutputChannels();

            if (channels.getAllChannels().isEmpty()) {
              // No data coming out of this stage. Report empty statistics, if the kernel is expecting statistics.
              if (stageDefinition.mustGatherResultKeyStatistics()) {
                listener.onDoneReadingInput(ClusterByStatisticsSnapshot.empty());
              } else {
                listener.onDoneReadingInput(null);
              }

              // Generate one empty channel so the next part of the pipeline has something to do.
              final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
              channel.writable().close();

              final OutputChannel outputChannel = OutputChannel.readOnly(
                  channel.readable(),
                  FrameWithPartition.NO_PARTITION
              );

              return new ResultAndChannels<>(
                  Futures.immediateFuture(null),
                  OutputChannels.wrap(Collections.singletonList(outputChannel))
              );
            } else if (stageDefinition.mustGatherResultKeyStatistics()) {
              return gatherResultKeyStatistics(channels);
            } else {
              // Report "done reading input" when the input future resolves.
              // No need to add any processors to the pipeline.
              resultAndChannels.resultFuture.addListener(
                  () -> listener.onDoneReadingInput(null),
                  Execs.directExecutor()
              );
              return resultAndChannels;
            }
          }
      );
    }

    /**
     * Add a {@link SuperSorter} using {@link StageDefinition#getSortKey()} and partition boundaries
     * from {@code partitionBoundariesFuture}.
     */
    public void globalSort(
        final OutputChannelFactory outputChannelFactory,
        final ListenableFuture<ClusterByPartitions> partitionBoundariesFuture
    )
    {
      pushAsync(
          resultAndChannels -> {
            final StageDefinition stageDefinition = workOrder.getStageDefinition();

            final File sorterTmpDir = frameContext.tempDir("super-sort");
            FileUtils.mkdirp(sorterTmpDir);
            if (!sorterTmpDir.isDirectory()) {
              throw new IOException("Cannot create directory: " + sorterTmpDir);
            }

            final WorkerMemoryParameters memoryParameters = frameContext.memoryParameters();
            final SuperSorter sorter = new SuperSorter(
                resultAndChannels.getOutputChannels().getAllReadableChannels(),
                stageDefinition.getFrameReader(),
                stageDefinition.getSortKey(),
                partitionBoundariesFuture,
                exec,
                outputChannelFactory,
                makeSuperSorterIntermediateOutputChannelFactory(sorterTmpDir),
                memoryParameters.getSuperSorterMaxActiveProcessors(),
                memoryParameters.getSuperSorterMaxChannelsPerProcessor(),
                -1,
                cancellationId,
                counterTracker.sortProgress(),
                removeNullBytes
            );

            return FutureUtils.transform(
                sorter.run(),
                sortedChannels -> new ResultAndChannels<>(Futures.immediateFuture(null), sortedChannels)
            );
          }
      );
    }

    /**
     * Add a {@link FrameChannelHashPartitioner} using {@link StageDefinition#getSortKey()}.
     */
    public void hashPartition(final OutputChannelFactory outputChannelFactory)
    {
      pushAsync(
          resultAndChannels -> {
            final ShuffleSpec shuffleSpec = workOrder.getStageDefinition().getShuffleSpec();
            final int partitions = shuffleSpec.partitionCount();

            final List<OutputChannel> outputChannels = new ArrayList<>();

            for (int i = 0; i < partitions; i++) {
              outputChannels.add(outputChannelFactory.openChannel(i));
            }

            final FrameChannelHashPartitioner partitioner = new FrameChannelHashPartitioner(
                resultAndChannels.getOutputChannels().getAllReadableChannels(),
                outputChannels.stream().map(OutputChannel::getWritableChannel).collect(Collectors.toList()),
                workOrder.getStageDefinition().getFrameReader(),
                workOrder.getStageDefinition().getClusterBy().getColumns().size(),
                FrameWriters.makeRowBasedFrameWriterFactory(
                    new ArenaMemoryAllocatorFactory(frameContext.memoryParameters().getStandardFrameSize()),
                    workOrder.getStageDefinition().getSignature(),
                    workOrder.getStageDefinition().getSortKey(),
                    removeNullBytes
                )
            );

            final ListenableFuture<Long> partitionerFuture = exec.runFully(partitioner, cancellationId);

            final ResultAndChannels<Long> retVal =
                new ResultAndChannels<>(partitionerFuture, OutputChannels.wrap(outputChannels));

            if (retVal.getOutputChannels().areReadableChannelsReady()) {
              return Futures.immediateFuture(retVal);
            } else {
              return FutureUtils.transform(partitionerFuture, ignored -> retVal);
            }
          }
      );
    }

    /**
     * Add a sequence of {@link SuperSorter}, operating on each current output channel in order, one at a time.
     */
    public void localSort(final OutputChannelFactory outputChannelFactory)
    {
      pushAsync(
          resultAndChannels -> {
            final StageDefinition stageDefinition = workOrder.getStageDefinition();
            final OutputChannels channels = resultAndChannels.getOutputChannels();
            final List<ListenableFuture<OutputChannel>> sortedChannelFutures = new ArrayList<>();

            ListenableFuture<OutputChannel> nextFuture = Futures.immediateFuture(null);

            for (final OutputChannel channel : channels.getAllChannels()) {
              final File sorterTmpDir = frameContext.tempDir(
                  StringUtils.format("hash-parts-super-sort-%06d", channel.getPartitionNumber())
              );

              FileUtils.mkdirp(sorterTmpDir);

              // SuperSorter will try to write to output partition zero; we remap it to the correct partition number.
              final OutputChannelFactory partitionOverrideOutputChannelFactory = new OutputChannelFactory()
              {
                @Override
                public OutputChannel openChannel(int expectedZero) throws IOException
                {
                  if (expectedZero != 0) {
                    throw new ISE("Unexpected part [%s]", expectedZero);
                  }

                  return outputChannelFactory.openChannel(channel.getPartitionNumber());
                }

                @Override
                public PartitionedOutputChannel openPartitionedChannel(String name, boolean deleteAfterRead)
                {
                  throw new UnsupportedOperationException();
                }

                @Override
                public OutputChannel openNilChannel(int expectedZero)
                {
                  if (expectedZero != 0) {
                    throw new ISE("Unexpected part [%s]", expectedZero);
                  }

                  return outputChannelFactory.openNilChannel(channel.getPartitionNumber());
                }
              };

              // Chain futures so we only sort one partition at a time.
              nextFuture = Futures.transformAsync(
                  nextFuture,
                  ignored -> {
                    final SuperSorter sorter = new SuperSorter(
                        Collections.singletonList(channel.getReadableChannel()),
                        stageDefinition.getFrameReader(),
                        stageDefinition.getSortKey(),
                        Futures.immediateFuture(ClusterByPartitions.oneUniversalPartition()),
                        exec,
                        partitionOverrideOutputChannelFactory,
                        makeSuperSorterIntermediateOutputChannelFactory(sorterTmpDir),
                        1,
                        2,
                        -1,
                        cancellationId,

                        // Tracker is not actually tracked, since it doesn't quite fit into the way we report counters.
                        // There's a single SuperSorterProgressTrackerCounter per worker, but workers that do local
                        // sorting have a SuperSorter per partition.
                        new SuperSorterProgressTracker(),
                        removeNullBytes
                    );

                    return FutureUtils.transform(sorter.run(), r -> Iterables.getOnlyElement(r.getAllChannels()));
                  },
                  MoreExecutors.directExecutor()
              );

              sortedChannelFutures.add(nextFuture);
            }

            return FutureUtils.transform(
                Futures.allAsList(sortedChannelFutures),
                sortedChannels -> new ResultAndChannels<>(
                    Futures.immediateFuture(null),
                    OutputChannels.wrap(sortedChannels)
                )
            );
          }
      );
    }

    /**
     * Return the (future) output channels for this pipeline.
     */
    public ListenableFuture<OutputChannels> build()
    {
      if (pipelineFuture == null) {
        throw new ISE("Not initialized");
      }

      return Futures.transformAsync(
          pipelineFuture,
          resultAndChannels ->
              Futures.transform(
                  resultAndChannels.getResultFuture(),
                  (Function<Object, OutputChannels>) input -> {
                    sanityCheckOutputChannels(resultAndChannels.getOutputChannels());
                    return resultAndChannels.getOutputChannels();
                  },
                  Execs.directExecutor()
              ),
          Execs.directExecutor()
      );
    }

    /**
     * Adds {@link KeyStatisticsCollectionProcessor}. Called by {@link #gatherResultKeyStatisticsAndReportDoneReadingInputIfNeeded()}.
     */
    private ResultAndChannels<?> gatherResultKeyStatistics(final OutputChannels channels)
    {
      final StageDefinition stageDefinition = workOrder.getStageDefinition();
      final List<OutputChannel> retVal = new ArrayList<>();
      final List<KeyStatisticsCollectionProcessor> processors = new ArrayList<>();

      for (final OutputChannel outputChannel : channels.getAllChannels()) {
        final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
        retVal.add(OutputChannel.readOnly(channel.readable(), outputChannel.getPartitionNumber()));

        processors.add(
            new KeyStatisticsCollectionProcessor(
                outputChannel.getReadableChannel(),
                channel.writable(),
                stageDefinition.getFrameReader(),
                stageDefinition.getClusterBy(),
                stageDefinition.createResultKeyStatisticsCollector(
                    frameContext.memoryParameters().getPartitionStatisticsMaxRetainedBytes()
                )
            )
        );
      }

      final ListenableFuture<ClusterByStatisticsCollector> clusterByStatisticsCollectorFuture =
          exec.runAllFully(
              ProcessorManagers.of(processors)
                               .withAccumulation(
                                   stageDefinition.createResultKeyStatisticsCollector(
                                       frameContext.memoryParameters().getPartitionStatisticsMaxRetainedBytes()
                                   ),
                                   ClusterByStatisticsCollector::addAll
                               ),
              // Run all processors simultaneously. They are lightweight and this keeps things moving.
              processors.size(),
              Bouncer.unlimited(),
              cancellationId
          );

      Futures.addCallback(
          clusterByStatisticsCollectorFuture,
          new FutureCallback<ClusterByStatisticsCollector>()
          {
            @Override
            public void onSuccess(final ClusterByStatisticsCollector result)
            {
              listener.onDoneReadingInput(result.snapshot());
            }

            @Override
            public void onFailure(Throwable t)
            {
              listener.onFailure(
                  new ISE(t, "Failed to gather clusterBy statistics for stage[%s]", stageDefinition.getId())
              );
            }
          },
          Execs.directExecutor()
      );

      return new ResultAndChannels<>(
          clusterByStatisticsCollectorFuture,
          OutputChannels.wrap(retVal)
      );
    }

    /**
     * Update the {@link #pipelineFuture}.
     */
    private void push(final ExceptionalFunction<ResultAndChannels<?>, ResultAndChannels<?>> fn)
    {
      pushAsync(
          channels ->
              Futures.immediateFuture(fn.apply(channels))
      );
    }

    /**
     * Update the {@link #pipelineFuture} asynchronously.
     */
    private void pushAsync(final ExceptionalFunction<ResultAndChannels<?>, ListenableFuture<ResultAndChannels<?>>> fn)
    {
      if (pipelineFuture == null) {
        throw new ISE("Not initialized");
      }

      pipelineFuture = FutureUtils.transform(
          Futures.transformAsync(
              pipelineFuture,
              fn::apply,
              Execs.directExecutor()
          ),
          resultAndChannels -> new ResultAndChannels<>(
              resultAndChannels.getResultFuture(),
              resultAndChannels.getOutputChannels().readOnly()
          )
      );
    }

    /**
     * Verifies there is exactly one channel per partition.
     */
    private void sanityCheckOutputChannels(final OutputChannels outputChannels)
    {
      for (int partitionNumber : outputChannels.getPartitionNumbers()) {
        final List<OutputChannel> outputChannelsForPartition =
            outputChannels.getChannelsForPartition(partitionNumber);

        Preconditions.checkState(partitionNumber >= 0, "Expected partitionNumber >= 0, but got [%s]", partitionNumber);
        Preconditions.checkState(
            outputChannelsForPartition.size() == 1,
            "Expected one channel for partition [%s], but got [%s]",
            partitionNumber,
            outputChannelsForPartition.size()
        );
      }
    }
  }

  private static class ResultAndChannels<T>
  {
    private final ListenableFuture<T> resultFuture;
    private final OutputChannels outputChannels;

    public ResultAndChannels(
        ListenableFuture<T> resultFuture,
        OutputChannels outputChannels
    )
    {
      this.resultFuture = resultFuture;
      this.outputChannels = outputChannels;
    }

    public ListenableFuture<T> getResultFuture()
    {
      return resultFuture;
    }

    public OutputChannels getOutputChannels()
    {
      return outputChannels;
    }
  }

  private interface ExceptionalFunction<T, R>
  {
    R apply(T t) throws Exception;
  }
}
