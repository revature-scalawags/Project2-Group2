import com.azure.core.credential.AzureKeyCredential
import com.azure.ai.textanalytics.models._
import com.azure.ai.textanalytics.TextAnalyticsClientBuilder
import com.azure.ai.textanalytics.TextAnalyticsClient
        
object SentimentAnalysis{
    final val KEY: String = "da18b5be9da74415b8118936d39f3f49"
    final val ENDPOINT: String = "https://twitteranalysisproject.cognitiveservices.azure.com/"
    final val client = authenticateClient(KEY, ENDPOINT);
    

    def authenticateClient(key: String, endpoint: String): TextAnalyticsClient = {

    return new TextAnalyticsClientBuilder()
        .credential(new AzureKeyCredential(key))
        .endpoint(endpoint)
        .buildClient();
    }

    def sentimentAnalysis(text: String): String ={
    val tweetSentiment: DocumentSentiment = client.analyzeSentiment(text)

    if(tweetSentiment.getConfidenceScores().getPositive >= 0.75) return "Positive"
    else if(tweetSentiment.getConfidenceScores().getNegative >= 0.75) return "Negative"
    else return "Mixed"
    }
}