import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.*;

public class JSONConverter {

    public static void main(String[] args) throws IOException, JSONException {

        JSONArray jsonArray = new JSONArray();
        /* Read temp result into the buffer */
        BufferedReader br = new BufferedReader(new FileReader("/Users/Peter/src/SentimentAnalysis/src/main/resources/output/part-r-00000"));
        String line = br.readLine(); /* Read per line */
        /* Convert the result to json inorder to use js to show the result */
        FileWriter fileWriter = new FileWriter("/Users/Peter/src/SentimentAnalysis/sentiment-visualization/data/result.json");
        /* Important steps */
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

        while (line != null) {
            JSONObject article = new JSONObject();
            /* fileName + sentiment + count */
            String[] title_emotion_count = line.split("\t");
            JSONObject emotionList = new JSONObject();
            /* sentiment + count */
            emotionList.put(title_emotion_count[1], title_emotion_count[2]);
            article.put("title", title_emotion_count[0]);
            for (int i = 0; i < 2; i++) { /* why for this loop */
                line = br.readLine();
                title_emotion_count = line.split("\t");
                emotionList.put(title_emotion_count[1], title_emotion_count[2]);
            }

            article.put("data", emotionList);
            jsonArray.put(article);

            line = br.readLine();
        }

        bufferedWriter.write(jsonArray.toString());

        br.close();
        bufferedWriter.close();

    }
}
