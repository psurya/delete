import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.net.InetAddress;


/**
 * Created by stp on 1/18/2017.
 */
public class DeleteByQuery {
   public static void main(String[] args) throws IOException, ParseException {

      String indexName = "sampleindex";
      String docType = "s";
      String searchTerm = "nike";

      Settings settings = Settings.settingsBuilder()
            .put("cluster.name", "elasticsearch").build();


      Client client = TransportClient.builder().settings(settings).build()
            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

      SearchRequestBuilder srb1 = client
            .prepareSearch().setQuery(QueryBuilders.queryStringQuery(searchTerm)).setSize(1000);


      MultiSearchResponse sr = client.prepareMultiSearch()
            .add(srb1)
            .execute().actionGet();
      BulkRequestBuilder bulkRequest = client.prepareBulk();

      // You will get all individual responses from MultiSearchResponse#getResponses()
      for (MultiSearchResponse.Item item : sr.getResponses()) {
         for(SearchHit hit : item.getResponse().getHits()){
            if(hit.getIndex().equalsIgnoreCase(indexName) && hit.getType().equalsIgnoreCase(docType)) {
               bulkRequest.add(client.prepareDelete(indexName, docType, hit.getId()));
            }
         }
      }

      if(bulkRequest.numberOfActions() > 0) {
         BulkResponse bulkResponse = bulkRequest.get();
         if (bulkResponse.hasFailures()) {
            System.out.println(bulkResponse.buildFailureMessage());
         } else {
            System.out.println("Successfully Deleted " + bulkRequest.numberOfActions() + " Entries");
         }
      }
   }
}
