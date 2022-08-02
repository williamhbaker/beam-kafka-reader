// mvn -e compile exec:java -Dexec.mainClass=org.apache.beam.samples.FromDekaf -Pdirect-runner -Dexec.args="--inputTopic=hello_world"

package org.apache.beam.samples;

import com.google.gson.Gson;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FromDekaf {
  private static final Logger LOG = LoggerFactory.getLogger(FromDekaf.class);
  private static final Gson GSON = new Gson();

  static final Duration FIVE_SECONDS = Duration.standardSeconds(5);
  static final Duration TEN_SECONDS = Duration.standardSeconds(10);

  static class ParseEventFn extends DoFn<String, GameActionInfo> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      GameActionInfo parsed = GSON.fromJson(c.element(), GameActionInfo.class);
      c.output(parsed);
    }
  }

  static class ExtractAndSumScore
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

    private final String field;

    ExtractAndSumScore(String field) {
      this.field = field;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> gameInfo) {
      return gameInfo
          .apply(
              MapElements.into(
                  TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                  .via((GameActionInfo gInfo) -> KV.of(gInfo.getKey(field), gInfo.getScore())))
          .apply(Sum.integersPerKey());
    }
  }

  static class CalculateTeamScores
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {
    private final Duration teamWindowDuration;
    private final Duration allowedLateness;

    CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
      this.teamWindowDuration = teamWindowDuration;
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> infos) {
      return infos
          .apply(
              "LeaderboardTeamFixedWindows",
              Window.<GameActionInfo>into(FixedWindows.of(teamWindowDuration))
                  .triggering(
                      AfterWatermark.pastEndOfWindow()
                          .withEarlyFirings(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(FIVE_SECONDS))
                          .withLateFirings(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(TEN_SECONDS)))
                  .withAllowedLateness(allowedLateness)
                  .accumulatingFiredPanes())
          .apply("ExtractTeamScore", new ExtractAndSumScore("team"));
    }
  }

  public interface Options extends StreamingOptions {
    @Description("Apache Kafka topic to read from.")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    @Description("Apache Kafka bootstrap servers in the form 'hostname:port'.")
    @Default.String("localhost:9092")
    String getBootstrapServer();

    void setBootstrapServer(String value);

    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(60)
    Integer getTeamWindowDuration();

    void setTeamWindowDuration(Integer value);

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(120)
    Integer getAllowedLateness();

    void setAllowedLateness(Integer value);
  }

  public static void main(final String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);

    var pipeline = Pipeline.create(options);

    PCollection<GameActionInfo> gameEvents = pipeline
        .apply("Read messages from Kafka",
            KafkaIO.<String, String>read()
                // Standard configurations.
                .withBootstrapServers(options.getBootstrapServer())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)

                // This sets the event times and watermarks for messages based on the
                // server-reported "LogAppendTime". Otherwise, the processing time is used.
                // There are other options for this behavior as well. The server must specify
                // the timestamp is LogAppendTime for this to work, see
                // https://github.com/estuary/dekaf/pull/1 for the emulator side.
                .withLogAppendTime()

                // This is for "exactly once" type of semantics added with Kafka 0.11. Currently
                // not supported by Dekaf.
                // .withReadCommitted()

                // commitOffsetsInFinalize() is like AUTO_COMMIT in that it allows for the
                // client to store commit reading progress on the server. It does not provide
                // any hard processing guarantees. Causes the client to send OffsetCommit
                // requests, which are currently a noop in Dekaf.
                .commitOffsetsInFinalize()

                // withConsumerConfigUpdates updates the consumer with configuration options.
                // There are many options - group.id must be set in order to use
                // commitOffsetsInFinalize. auto.offset.reset=earliest is also interesting for
                // starting to read messages at the beginning, doing a "backfill".
                .withConsumerConfigUpdates(Map.of("group.id", "some-group"))

                // Drops the kafka metadata for processing.
                .withoutMetadata())

        // Other interesting configs/todos:

        // withStartReadTime & withStartReadTime - Only read events with timestamps in a
        // certain window. Presumably the Emulator would need to be able to handle
        // offset requests other than -1 and -2 that would represent an actual time in
        // order for this to work.

        // Just gets the message as a JSON string.
        .apply("Get message contents", Values.<String>create())

        // Unmarshal the string data into a GameActionInfo.
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()));

    // Calculate aggregate scores in windowed batches:
    // - New messages are read as they are available from the server.
    // - Windows of time are started to contain sets of messages.
    // - The aggregation logic sums the scores for each "team" continuously.
    // - Once the window duration amount of time has elapsed, the window is output
    // and can be handled further by the pipeline.
    PCollection<KV<String, Integer>> teamScores = gameEvents
        .apply(
            "CalculateTeamScores",
            new CalculateTeamScores(
                Duration.standardMinutes(options.getTeamWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())));

    // Just log the messages in this simple example.
    teamScores
        .apply("Log messages", MapElements.into(TypeDescriptors.strings())
            .via(ts -> {
              LOG.info("****** team: {} score: {}", ts.getKey(), ts.getValue());
              return ts.getKey();
            }));

    pipeline.run();
  }
}
