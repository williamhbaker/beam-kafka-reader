package org.apache.beam.samples;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class GameActionInfo {
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