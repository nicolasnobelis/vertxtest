package nino;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.impl.ServiceTypes;
import io.vertx.servicediscovery.spi.ServiceType;
import io.vertx.servicediscovery.types.EventBusService;

public class OutVerticle extends AbstractVerticle {

    private static final String EVENT_BUS_ADDRESS = "OUT";

    private HttpClient httpClient;
    @Override
    public void start() throws Exception {
        System.out.println("OUT(START): Starting OutVerticle");

        vertx.eventBus().consumer( EVENT_BUS_ADDRESS, this::handle );

        httpClient = vertx.createHttpClient();

        ServiceDiscovery discovery = ServiceDiscovery.create( vertx );
        Record record = new Record()
                .setType( ServiceType.UNKNOWN )
                .setLocation( new JsonObject()
                        .put( "endpoint", "nino.bus.Out" )
                        .put( "event.bus", EVENT_BUS_ADDRESS ) )
                .setName( "out" );

        discovery.publish( record, event -> {
            if (event.succeeded()) {
                Record publishedRecord = event.result();
                System.out.printf("OUT(START): I published my service !%n");
            } else {
                event.cause().printStackTrace();
            }
            discovery.close();
        } );
    }

    private void handle(Message<Object> message) {

        JsonObject body = ( JsonObject ) message.body();
        Long correlationId = body.getLong( "correlationId" );

        System.out.printf("OUT(%d): received a message from the bus !%n", correlationId);
        System.out.printf("OUT(%d): payload : %s%n", correlationId, body.getString( "message" ));

        vertx.setTimer( 2000, aLong -> {
            System.out.printf("OUT(%d): waited enough. Sending the HTTP request%n", correlationId);

            HttpClient httpClient = vertx.createHttpClient();
            httpClient.get( 9999, "localhost", "/?correlationId=" + correlationId, response -> {
                System.out.printf("OUT(%d): received response : %s%n%n", correlationId, response.statusCode());
            } ).end();

            System.out.printf("OUT(%d): request sent%n", correlationId);
        } );
    }


    @Override
    public void stop() throws Exception {
        System.out.println("OUT: Stopping OutVerticle");
    }
}
