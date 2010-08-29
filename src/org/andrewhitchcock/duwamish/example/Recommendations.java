package org.andrewhitchcock.duwamish.example;

import java.util.Random;

import org.andrewhitchcock.duwamish.Context;
import org.andrewhitchcock.duwamish.Duwamish;
import org.andrewhitchcock.duwamish.accumulator.DoubleSumAccumulator;
import org.andrewhitchcock.duwamish.example.protos.Examples.Feature;
import org.andrewhitchcock.duwamish.example.protos.Examples.FeatureMessage;
import org.andrewhitchcock.duwamish.example.protos.Examples.RecommendationsMessage;
import org.andrewhitchcock.duwamish.example.protos.Examples.RecommendationsVertexData;
import org.andrewhitchcock.duwamish.model.Edge;
import org.andrewhitchcock.duwamish.model.Vertex;

public class Recommendations {
  
  public static class RecommendationsVertex extends Vertex<RecommendationsVertexData, FeatureMessage, RecommendationsMessage> {
    
    static final int runsPerFeature = 50;
    static final int featureCount = 10;
    static final double learningRate = 0.001f;
    static final double ratingCapMax = 10.0f;
    static final double ratingCapMin = -10.0f;
    
    @Override
    public RecommendationsVertexData compute(String vertexId, RecommendationsVertexData value, Iterable<RecommendationsMessage> messages, Context<RecommendationsVertexData, FeatureMessage, RecommendationsMessage> context) {
      RecommendationsVertexData.Builder resultBuilder = value.toBuilder();
      
      int currentRound = (int) Math.floor(context.getSuperstepNumber() / 2);
      int currentFeature = (int) Math.floor(currentRound / runsPerFeature);
      
      boolean isRatingRound = (context.getSuperstepNumber() & 1L) == 1;
      boolean isRatingVertex = value.hasRating();

      if (!isRatingRound && !isRatingVertex) {
        if (vertexId.equals("m1")) {
          System.out.println(currentFeature + " " + value.getFeatureVector(currentFeature));
        }
        
        // even rounds are for updating features and then broadcasting the new value
        int receivingUpdatesForFeature = currentFeature;
        
        // if this is the first run of the new round, we need to save using the last feature
        if (currentRound % runsPerFeature == 0 && currentRound != 0) {
          receivingUpdatesForFeature--;
        }
        
        double newValue = value.getFeatureVector(receivingUpdatesForFeature);
        
        for (RecommendationsMessage message : messages) {
          newValue += message.getValue();
        }

        newValue = Math.max(Math.min(newValue, ratingCapMax), ratingCapMin);
        resultBuilder.setFeatureVector(receivingUpdatesForFeature, newValue);
        
        for (Edge<FeatureMessage> edge : context.getEdgeIterable()) {
          context.sendMessageTo(
              edge.getTargetVertexId(),
              RecommendationsMessage.newBuilder()
                .setValue(receivingUpdatesForFeature == currentFeature ? newValue : value.getFeatureVector(currentFeature))
                .setFeature(value.getFeature())
                .build());
        }
      } else if (isRatingRound && isRatingVertex) {
        // odd rounds are for calculating the dot product prediction and broadcasting the diff
        Double userMessage = null;
        Double movieMessage = null;
        
        for (RecommendationsMessage message : messages) {
          if (message.getFeature() == Feature.USER) {
            userMessage = message.getValue();
          } else if (message.getFeature() == Feature.MOVIE) {
            movieMessage = message.getValue();
          }
        }
        
        double prediction = value.getResidue() + (userMessage * movieMessage);
        double error = learningRate * (value.getRating() - prediction);
        
        double userUpdate = error * movieMessage;
        double movieUpdate = error * userMessage;
        
        for (Edge<FeatureMessage> edge : context.getEdgeIterable()) {
          RecommendationsMessage.Builder messageBuilder = RecommendationsMessage.newBuilder();
          if (edge.getValue().getFeature() == Feature.USER) {
            messageBuilder.setValue(userUpdate);
          } else if (edge.getValue().getFeature() == Feature.MOVIE) {
            messageBuilder.setValue(movieUpdate);
          }
          context.sendMessageTo(edge.getTargetVertexId(), messageBuilder.build());
        }
        
        // if this is the last round for this feature we need to update our residue
        if ((currentRound + 1) % runsPerFeature == 0) {
          resultBuilder.setResidue(value.getResidue() + prediction);
        }    

        context.emitAccumulation("MSE", error * error);
      }
      
      return resultBuilder.build();
    }
  }
  
  private static RecommendationsVertexData newVertex(Feature feature) {
    RecommendationsVertexData.Builder builder = RecommendationsVertexData.newBuilder();
    for (int i = 0; i < RecommendationsVertex.featureCount; i++) {
      builder.addFeatureVector(1.0);
    }
    return builder.setFeature(feature).build();
  }
  
  private static RecommendationsVertexData newVertex(int rating) {
    return RecommendationsVertexData.newBuilder().setRating(rating).setResidue(0.0).build();
  }
  
  public static void main(String[] args) throws Exception {
    Random random = new Random();
    
    int movieCount = 256;
    int userCount = 2048;
    int maxRatingsPerUser = 64;
    
    int maxRating = 5;
    
    if (args.length == 4) {
      movieCount = Integer.parseInt(args[0]);
      userCount = Integer.parseInt(args[1]);
      maxRatingsPerUser = Integer.parseInt(args[2]);
      random.setSeed(Long.parseLong(args[3]));
    }
    
    Duwamish<RecommendationsVertex, RecommendationsVertexData, FeatureMessage, RecommendationsMessage> duwamish = Duwamish.newBuilder()
      .withVertex(RecommendationsVertex.class)
      .withVertexType(RecommendationsVertexData.class)
      .withEdgeType(FeatureMessage.class)
      .withMessageType(RecommendationsMessage.class)
      .build();
    duwamish.addAccumulator("MSE", new DoubleSumAccumulator());
    
    for (int i = 0; i < movieCount; i++) {
      duwamish.addVertex("m" + Integer.toString(i), newVertex(Feature.MOVIE));
    }
    
    int totalRatingCount = 0;
    for (int i = 0; i < userCount; i++) {
      String userId = "u" + Integer.toString(i);
      duwamish.addVertex(userId, newVertex(Feature.USER));
      
      int ratingsForThisUser = random.nextInt(maxRatingsPerUser);
      for (int j = 0; j < ratingsForThisUser; j++) {
        String ratingId = "r" + Integer.toString(totalRatingCount);
        String movieId = "m" + Integer.toString(random.nextInt(movieCount));

        duwamish.addVertex(ratingId, newVertex(random.nextInt(maxRating)));
        
        duwamish.addEdge(userId, ratingId, null);
        duwamish.addEdge(ratingId, userId, FeatureMessage.newBuilder().setFeature(Feature.USER).build());
        duwamish.addEdge(movieId, ratingId, null);
        duwamish.addEdge(ratingId, movieId, FeatureMessage.newBuilder().setFeature(Feature.MOVIE).build());
        
        totalRatingCount++;
      }
    }
    
    duwamish.run(RecommendationsVertex.featureCount * RecommendationsVertex.runsPerFeature * 2);
  }
}
