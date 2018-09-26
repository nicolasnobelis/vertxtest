package nino;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.types.EventBusService;

import java.util.HashMap;
import java.util.Map;

public class InVerticle extends AbstractVerticle {
    private HttpServer httpServer;
    private static long idSeed = 1;

    private String outBusAddress;

    @Override
    public void start() throws Exception {
        System.out.println("IN(START): Starting InVerticle");
        httpServer = vertx.createHttpServer()
                .requestHandler( this::handle )
                .listen( 9999 );
        System.out.println("IN(START): HTTP Server started");

        ServiceDiscovery discovery = ServiceDiscovery.create( vertx );
        discovery.getRecord( record -> "nino.bus.Out".equals( record.getLocation().getString( "endpoint" ) ), ar -> {
            if ( ar.succeeded() ) {
                if (ar.result() == null) {
                    System.out.printf("IN(START): No service found%n");
                } else {
                    System.out.printf("IN(START): I found the service at=%s !%n", ar.result().getLocation());
                    outBusAddress = ar.result().getLocation().getString(  "event.bus" );
                }
            } else {
                ar.cause().printStackTrace();
            }
            discovery.close();
        } );



        if (outBusAddress == null) {
            throw new IllegalStateException( "No service found !" );
        }
    }

    private void handle(HttpServerRequest request) {
        if (request.uri().contains( "favicon" )) {
            System.out.println("IN: ignore the favicon request");

            sendResponse200( request,"OK OK favicon" );
            return;
        }

        String previousId = request.params().get( "correlationId" );

        long correlationId = idSeed++;

        System.out.printf( "IN(%d) : Request received for previous id=%s %n", correlationId, previousId );

        sendResponse200( request,"OK, I received the request" );

        vertx.setTimer( 2000, aLong -> {
            System.out.printf("IN(%d): waited enough. Publishing on the bus%n", correlationId);
            JsonObject payload = new JsonObject()
                    .put( "correlationId", correlationId )
                    .put( "message", "coucou" );
            vertx.eventBus().publish( outBusAddress, payload );
        } );
    }

    private void sendResponse200(HttpServerRequest request, String payload) {
        HttpServerResponse response = request.response();
        response.setStatusCode( 200 )
                .headers()
                .add( "Content-Type", "text/plain" )
                .add( "Content-Length", String.valueOf( payload.length() ) );
        response.write( payload )
                .end();
    }


    @Override
    public void stop() throws Exception {
        System.out.println("IN: Stopping InVerticle");
    }
}
