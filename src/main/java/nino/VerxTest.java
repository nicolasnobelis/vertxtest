package nino;

import io.vertx.core.Vertx;
import io.vertx.servicediscovery.ServiceDiscovery;

public class VerxTest {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        vertx.deployVerticle( new OutVerticle(), event -> {
            vertx.deployVerticle( new InVerticle() );
        } );
    }
}
