package org.andrewhitchcock.duwamish.example;

import java.util.List;
import java.util.Random;

import org.andrewhitchcock.duwamish.Context;
import org.andrewhitchcock.duwamish.Duwamish;
import org.andrewhitchcock.duwamish.accumulator.DoubleSumAccumulator;
import org.andrewhitchcock.duwamish.model.Edge;
import org.andrewhitchcock.duwamish.model.Vertex;

import com.google.common.collect.Lists;

public class Recommendations {
  enum FeatureType {
    Movie,
    User;
  }
  
  public static void main(String[] args) throws Exception {
    Random random = new Random();
    
    final int runsPerFeature = 50;
    final int featureCount = 10;
    final double learningRate = 0.001f;
    final double ratingCapMax = 10.0f;
    final double ratingCapMin = -10.0f;
    
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
    
    class VertexData {
      // Users and movies keep a feature vector and their type
      List<Double> featureVector;
      FeatureType featureType;
      
      // Ratings keep a rating and residue
      Integer rating;
      Double residue;
      
      public VertexData(FeatureType featureType) {
        featureVector = Lists.newArrayListWithCapacity(featureCount);
        for (int i = 0; i < featureCount; i++) {
          featureVector.add(i, 1.0);
        }
        this.featureType = featureType;
      }
      
      public VertexData(Integer rating) {
        this.rating = rating;
        residue = 0.0;
      }
    }
    
    class MessageData {
      Double value;
      FeatureType type;
      
      public MessageData(Double value, FeatureType type) {
        this.value = value;
        this.type = type;
      }
    }
    
    class RecommendationsVertex extends Vertex<VertexData, FeatureType, MessageData> {

      public RecommendationsVertex(String id, VertexData vertexData) {
        super(id);
        setValue(vertexData);
      }
      
      @Override
      public void compute(Iterable<MessageData> messages, Context<VertexData, FeatureType, MessageData> context) {
        VertexData data = getValue();
        
        int currentRound = (int) Math.floor(context.getSuperstepNumber() / 2);
        int currentFeature = (int) Math.floor(currentRound / runsPerFeature);
        
        boolean isRatingRound = (context.getSuperstepNumber() & 1L) == 1;
        boolean isRatingVertex = data.rating != null;

        if (!isRatingRound && !isRatingVertex) {
          if (getVertexId().equals("m1")) {
            System.out.println(currentFeature + " " + data.featureVector.get(currentFeature));
          }
          
          // even rounds are for updating features and then broadcasting the new value
          int receivingUpdatesForFeature = currentFeature;
          
          // if this is the first run of the new round, we need to save using the last feature
          if (currentRound % runsPerFeature == 0 && currentRound != 0) {
            receivingUpdatesForFeature--;
          }
          
          double newValue = data.featureVector.get(receivingUpdatesForFeature);
          
          for (MessageData message : messages) {
            newValue += message.value;
          }

          newValue = Math.max(Math.min(newValue, ratingCapMax), ratingCapMin);
          data.featureVector.set(receivingUpdatesForFeature, newValue);
          
          MessageData outputMessage = new MessageData(data.featureVector.get(currentFeature), data.featureType);
          for (Edge<FeatureType> edge : context.getEdgeIterable()) {
            context.sendMessageTo(edge.getTargetVertexId(), outputMessage);
          }
        } else if (isRatingRound && isRatingVertex) {
          // odd rounds are for calculating the dot product prediction and broadcasting the diff
          Double userMessage = null;
          Double movieMessage = null;
          
          for (MessageData message : messages) {
            if (message.type == FeatureType.User) {
              userMessage = message.value;
            } else if (message.type == FeatureType.Movie) {
              movieMessage = message.value;
            }
          }
          
          double prediction = data.residue + (userMessage * movieMessage);
          double error = learningRate * (data.rating - prediction);
          
          double userUpdate = error * movieMessage;
          double movieUpdate = error * userMessage;
          
          for (Edge<FeatureType> edge : context.getEdgeIterable()) {
            if (edge.getValue() == FeatureType.User) {
              context.sendMessageTo(edge.getTargetVertexId(), new MessageData(userUpdate, null));
            } else if (edge.getValue() == FeatureType.Movie) {
              context.sendMessageTo(edge.getTargetVertexId(), new MessageData(movieUpdate, null));
            }
          }
          
          // if this is the last round for this feature we need to update our residue
          if ((currentRound + 1) % runsPerFeature == 0) {
            data.residue += prediction;
          }    

          context.emitAccumulation("MSE", error * error);
        } else { 
          // this vertex has nothing to do this round
          return;
        }
        
        setValue(data);
      }
    }
    
    Duwamish<VertexData, FeatureType, MessageData> duwamish = Duwamish.createWithPartitionCount(32);
    duwamish.addAccumulator("MSE", new DoubleSumAccumulator());
    
    for (int i = 0; i < movieCount; i++) {
      duwamish.addVertex(new RecommendationsVertex("m" + Integer.toString(i), new VertexData(FeatureType.Movie)));
    }
    
    int totalRatingCount = 0;
    for (int i = 0; i < userCount; i++) {
      String userId = "u" + Integer.toString(i);
      duwamish.addVertex(new RecommendationsVertex(userId, new VertexData(FeatureType.User)));
      
      int ratingsForThisUser = random.nextInt(maxRatingsPerUser);
      for (int j = 0; j < ratingsForThisUser; j++) {
        String ratingId = "r" + Integer.toString(totalRatingCount);
        String movieId = "m" + Integer.toString(random.nextInt(movieCount));

        duwamish.addVertex(new RecommendationsVertex(ratingId, new VertexData(random.nextInt(maxRating))));
        
        duwamish.addEdge(userId, new Edge<FeatureType>(ratingId, null));
        duwamish.addEdge(ratingId, new Edge<FeatureType>(userId, FeatureType.User));
        duwamish.addEdge(movieId, new Edge<FeatureType>(ratingId, null));
        duwamish.addEdge(ratingId, new Edge<FeatureType>(movieId, FeatureType.Movie));
        
        totalRatingCount++;
      }
    }
    
    duwamish.run(featureCount * runsPerFeature * 2);
  }
}
