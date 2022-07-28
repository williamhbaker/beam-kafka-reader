// run like mvn -e compile exec:java -Dexec.mainClass=org.apache.beam.samples.KafkaToBigQuery -Dexec.args="--inputTopic=hello_world"

package org.apache.beam.samples;

import com.google.gson.Gson;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithTimestamps;
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
import org.joda.time.Instant;
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

  @DefaultCoder(AvroCoder.class)
  static class GameActionInfo {
    String User;
    String Team;
    Integer Score;
    String FinishedAt;

    public GameActionInfo() {
    }

    public String getUser() {
      return this.User;
    }

    public String getTeam() {
      return this.Team;
    }

    public Integer getScore() {
      return this.Score;
    }

    public String getTimestamp() {
      return this.FinishedAt;
    }

    public String getKey(String keyname) {
      if ("team".equals(keyname)) {
        return this.Team;
      } else { // return username as default
        return this.User;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || o.getClass() != this.getClass()) {
        return false;
      }

      GameActionInfo gameActionInfo = (GameActionInfo) o;

      if (!this.getUser().equals(gameActionInfo.getUser())) {
        return false;
      }

      if (!this.getTeam().equals(gameActionInfo.getTeam())) {
        return false;
      }

      if (!this.getScore().equals(gameActionInfo.getScore())) {
        return false;
      }

      return this.getTimestamp().equals(gameActionInfo.getTimestamp());
    }

    @Override
    public int hashCode() {
      return Objects.hash(User, Team, Score, FinishedAt);
    }
  }

  public static class ExtractAndSumScore
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
    @Default.String("localhost:9091")
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
                .withBootstrapServers(options.getBootstrapServer())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)

                // Figure out how to set the timestamp type, probably something in the
                // protocol..?
                // .withLogAppendTime()

                // The broker does not support LIST_OFFSETS with version in range [2,7]. The
                // supported range is [0,1].
                // .withReadCommitted()

                .withConsumerConfigUpdates(Map.of("auto.offset.reset", "earliest"))
                // .commitOffsetsInFinalize()

                .withoutMetadata())
        .apply("Get message contents", Values.<String>create())
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()))
        .apply("Add processing time", WithTimestamps
            .of((gameInfo) -> new Instant(gameInfo.getTimestamp())));

    // Calculate scores
    PCollection<KV<String, Integer>> teamScores = gameEvents
        .apply(
            "CalculateTeamScores",
            new CalculateTeamScores(
                Duration.standardMinutes(options.getTeamWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())));

    // Log messages
    teamScores
        .apply("Log messages", MapElements.into(TypeDescriptors.strings())
            .via(ts -> {
              LOG.info("****** team: {} score: {}", ts.getKey(), ts.getValue());
              return ts.getKey();
            }));

    pipeline.run();
  }
}
