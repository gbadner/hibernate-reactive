package org.hibernate.rx.impl;

import io.vertx.axle.pgclient.PgPool;
import io.vertx.axle.sqlclient.Row;
import io.vertx.axle.sqlclient.RowSet;
import io.vertx.axle.sqlclient.Tuple;
import org.hibernate.rx.RxSession;
import org.hibernate.rx.service.RxConnection;

import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * A reactive connection pool for PostgreSQL
 */
public class PgPoolConnection implements RxConnection {

	private final PgPool pool;
	private final boolean showSQL;

	public PgPoolConnection(PgPool pool, boolean showSQL) {
		this.pool = pool;
		this.showSQL = showSQL;
	}

	@Override
	public CompletionStage<Void> inTransaction(
			Consumer<RxSession> consumer,
			RxSession delegate) {
		// Not used at the moment
		// Just an idea
//		return CompletableFuture.runAsync( () -> {
//			pool.getConnection( res -> {
//				if (res.succeeded()) {
//					// Transaction must use a connection
//					SqlConnection conn = res.result();
//
//					// Begin the transaction
//					Transaction tx = conn.begin();
//
//					// Commit the transaction
//					tx.commit(ar -> {
//						consumer.accept( delegate );
//					});
//				}
//			});
//		} );
		return null;
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return preparedQuery( sql ).thenApply( res -> res.rowCount() );
	}

	@Override
	public CompletionStage<Integer> update(String sql, Tuple parameters) {
		return preparedQuery( sql, parameters ).thenApply( res -> res.rowCount() );
	}

	@Override
	public CompletionStage<RowSet<Row>> preparedQuery(String sql, Tuple parameters) {
		if (showSQL) {
			System.out.println(sql);
		}
		return pool.preparedQuery( sql, parameters );
	}

	@Override
	public CompletionStage<RowSet<Row>> preparedQuery(String sql) {
		if (showSQL) {
			System.out.println(sql);
		}
		return pool.preparedQuery( sql );
	}

	@Override
	public void close() {
		// Nothing to do here, I think
	}
}
