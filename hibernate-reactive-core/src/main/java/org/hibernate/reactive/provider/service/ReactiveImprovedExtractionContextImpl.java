/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CompletionStage;

import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.resource.transaction.spi.DdlTransactionIsolator;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.tool.schema.internal.exec.ImprovedExtractionContextImpl;
import org.hibernate.tool.schema.internal.exec.JdbcContext;

import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

public class ReactiveImprovedExtractionContextImpl extends ImprovedExtractionContextImpl {

	private static CoreMessageLogger LOG = CoreLogging.messageLogger( ReactiveImprovedExtractionContextImpl.class );

	private final ReactiveConnectionPool service;

	public ReactiveImprovedExtractionContextImpl(
			ServiceRegistry registry,
			Namespace.Name name,
			DatabaseObjectAccess databaseObjectAccess) {
		super(
				registry,
				registry.getService( JdbcEnvironment.class ),
				new DdlTransactionIsolator() {
					@Override
					public JdbcContext getJdbcContext() {
						return null;
					}

					@Override
					public void prepare() {
					}

					@Override
					public Connection getIsolatedConnection() {
						return null;
					}

					@Override
					public void release() {
					}
				},
				name.getCatalog(),
				name.getSchema(),
				databaseObjectAccess
		);
		service = registry.getService( ReactiveConnectionPool.class );
	}

	@Override
	public <T> T getQueryResults(
			String queryString,
			ResultSetProcessor<T> resultSetProcessor) throws SQLException {

		final CompletionStage<ReactiveConnection> connectionStage = service.getConnection();

		try (final ResultSet resultSet = getQueryResultSet( queryString, connectionStage ) ) {
			return resultSetProcessor.process( resultSet );
		}
		finally {
			connectionStage.whenComplete( (c, e) -> c.close() );
		}
	}

	private ResultSet getQueryResultSet(String queryString, CompletionStage<ReactiveConnection> connectionStage) {
		return connectionStage.thenCompose( c -> c.selectJdbcOutsideTransaction( queryString ) )
				.handle( (resultSet, err) -> {
					logSqlException(
							err,
							() -> "could not execute query ", queryString
					);
					return returnOrRethrow( err, resultSet );
				} )
				.toCompletableFuture()
				.join();
	}
}
