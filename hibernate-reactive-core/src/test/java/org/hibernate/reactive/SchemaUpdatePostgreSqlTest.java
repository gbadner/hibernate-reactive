/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.ForeignKey;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Index;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.testing.SessionFactoryManager;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.tool.schema.JdbcMetadaAccessStrategy;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import io.vertx.core.Promise;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunnerWithParametersFactory;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * @author Gail Badner
 */

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(VertxUnitRunnerWithParametersFactory.class)
public class SchemaUpdatePostgreSqlTest extends AbstractReactiveTest {

	@Parameterized.Parameters
	public static Collection<String> parameters() {
		return Arrays.asList(
				JdbcMetadaAccessStrategy.GROUPED.toString(), JdbcMetadaAccessStrategy.INDIVIDUALLY.toString()
		);
	}

	@Parameterized.Parameter
	public String jdbcMetadataExtractorStrategy;

	public static SessionFactoryManager factoryManager = new SessionFactoryManager();

	@Rule
	public DatabaseSelectionRule dbRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

    private int aSimpleId;

	@Test
	public void testUpdate(TestContext context) {

		final String indexDefinitionQuery =
				"select indexdef from pg_indexes where schemaname = 'public' and tablename = ? order by indexname";
		final String foreignKeyDefinitionQuery =
				"select pg_catalog.pg_get_constraintdef(f.oid, true) as condef " +
						"from pg_catalog.pg_constraint f, pg_catalog.pg_class c " +
						"where f.conname = ? and c.oid = f.conrelid and c.relname = ?";

		final ASimpleNext aSimple = new ASimpleNext();
		aSimple.aValue = 9;
		aSimple.aStringValue = "abc";
		aSimple.data = "Data";

		final AOther aOther = new AOther();
		aOther.id1 = 1;
		aOther.id2 = "other";
		aOther.anotherString = "another";

		final AAnother aAnother = new AAnother();
		aAnother.description = "description";

		aSimple.aOther = aOther;
		aSimple.aAnother = aAnother;

		test(
				context,
				setup(
						Arrays.asList( ASimpleNext.class, AOther.class, AAnother.class ),
						"update",
						false
				)
				.thenCompose( v -> getSessionFactory().withTransaction( (session, t) -> session
								.persist( aSimple )
								.thenApply( v1 -> aSimpleId = aSimple.id )
				) )
				.thenCompose( v1 -> openSession()
						.thenCompose( s -> s.find( ASimpleNext.class, aSimpleId )
								.thenApply( result -> {
									context.assertNotNull( result );
									context.assertEquals( aSimple.aValue, result.aValue );
									context.assertEquals( aSimple.aStringValue, result.aStringValue );
									context.assertEquals( aSimple.data, result.data );
									context.assertNotNull( result.aOther );
									context.assertEquals( aOther.id1, result.aOther.id1 );
									context.assertEquals( aOther.id2, result.aOther.id2 );
									context.assertEquals( aOther.anotherString, result.aOther.anotherString );
									context.assertNotNull( result.aAnother );
									context.assertEquals( aAnother.description, result.aAnother.description );
									return s;
								} )
						)
						// check that indexes for asimple were created properly
						.thenCompose( s -> s.createNativeQuery( indexDefinitionQuery, String.class )
								.setParameter( 1, "asimple" )
								.getResultList()
								.thenAccept( list -> {
									context.assertEquals(
										list.get( 0 ),
											"CREATE UNIQUE INDEX asimple_pkey ON public.asimple USING btree (id)"
									);
									context.assertEquals(
											list.get( 1 ),
											"CREATE INDEX i_asimple_avalue_astringvalue ON public.asimple USING btree (avalue, astringvalue DESC)"
									);
									context.assertEquals(
											list.get( 2 ),
											"CREATE INDEX i_asimple_avalue_data ON public.asimple USING btree (avalue DESC, data)"
									);
									context.assertEquals(
											list.get( 3 ),
											"CREATE UNIQUE INDEX u_asimple_astringvalue ON public.asimple USING btree (astringvalue)"
									);
								} )
								.thenApply( v -> s )
						)
						.thenCompose( s -> s.createNativeQuery( indexDefinitionQuery, String.class )
								.setParameter( 1, "aother" )
								.getSingleResult()
								.thenAccept( result ->
									context.assertEquals(
											result,
											"CREATE UNIQUE INDEX aother_pkey ON public.aother USING btree (id1, id2)"
									)
								)
								.thenApply( v -> s )
						)
						.thenCompose( s -> s.createNativeQuery( indexDefinitionQuery, String.class )
								.setParameter( 1, "aanother" )
								.getSingleResult()
								.thenAccept( result ->
													 context.assertEquals(
															 result,
															 "CREATE UNIQUE INDEX aanother_pkey ON public.aanother USING btree (id)"
													 )
								)
								.thenApply( v -> s )
						)
						// check foreign keys
						.thenCompose( s -> s.createNativeQuery( foreignKeyDefinitionQuery, String.class )
								.setParameter( 1, "fk_asimple_aother" )
								.setParameter( 2, "asimple" )
								.getSingleResult()
								.thenAccept( result ->
													 context.assertEquals(
													 		result,
															"FOREIGN KEY (id1, id2) REFERENCES aother(id1, id2)"
													 )
								)
								.thenApply( v -> s )
						)
						.thenCompose( s -> s.createNativeQuery( foreignKeyDefinitionQuery, String.class )
								.setParameter( 1, "fk_asimple_aanother" )
								.setParameter( 2, "asimple" )
								.getSingleResult()
								.thenAccept( result ->
													 context.assertEquals(
															 result,
															 "FOREIGN KEY (aanother_id) REFERENCES aanother(id)"
													 )
								)
								.thenApply( v -> s )
						)
				)
		);
	}

	@Before
	public void before(TestContext context) {
		test(
				context,
				setup(
						Arrays.asList( ASimpleFirst.class, AOther.class ),
						"create",
						true
				)
		);
	}

	@After
	public void after(TestContext context) {
		test(
				context,
				deleteEntities( "ASimple", AOther.class.getName(), AAnother.class.getName() )
						.thenCompose( v1 -> factoryManager.stop() )
				.thenCompose( v -> setup(
						Arrays.asList( ASimpleNext.class, AOther.class, AAnother.class ),
						"drop",
						true
				) )
		);
	}

	protected SessionFactoryManager getSessionFactoryManager() {
		return factoryManager;
	}

	private CompletionStage<Void> setup(
			Collection<Class<?>> entityClasses,
			String hbm2DdlOption,
			boolean closeAfterSessionFactoryStarted) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		vertxContextRule.vertx()
				.executeBlocking(
						// schema generation is a blocking operation and so it causes an
						// exception when run on the Vert.x event loop. So call it using
						// Vertx.executeBlocking()
						p -> startFactoryManager( p, entityClasses, hbm2DdlOption ),
						event -> {
							if ( closeAfterSessionFactoryStarted ) {
								factoryManager.stop();
							}
							if ( event.succeeded() ) {
								future.complete( null );
							}
							else {
								future.completeExceptionally( event.cause() );
							}
						}
				);
		return future;
	}

	private void startFactoryManager(Promise<Object> p, Collection<Class<?>> entityClasses, String hbm2DdlOption) {
		try {
			factoryManager.start( () -> createHibernateSessionFactory( entityClasses, hbm2DdlOption ) );
			p.complete();
		}
		catch (Throwable e) {
			p.fail( e );
		}
	}

	private SessionFactory createHibernateSessionFactory(Collection<Class<?>> entityClasses, String hbm2DdlOption) {
		Configuration configuration = constructConfiguration( hbm2DdlOption );
		for ( Class<?> entityClass : entityClasses ) {
			configuration.addAnnotatedClass( entityClass );
		}
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.addService( VertxInstance.class, (VertxInstance) () -> vertxContextRule.vertx() )
				.applySettings( configuration.getProperties() );
		StandardServiceRegistry registry = builder.build();
		configureServices( registry );
		SessionFactory sessionFactory = configuration.buildSessionFactory( registry );
		return sessionFactory;
	}

	protected Configuration constructConfiguration(String hbm2DdlOption) {
		Configuration configuration = constructConfiguration();
		configuration.setProperty( Settings.HBM2DDL_AUTO, hbm2DdlOption );
		configuration.setProperty( Settings.DEFAULT_SCHEMA, "public" );
		configuration.setProperty( Settings.HBM2DDL_JDBC_METADATA_EXTRACTOR_STRATEGY, jdbcMetadataExtractorStrategy );

		return configuration;
	}


	@Entity(name = "ASimple")
	@Table(name = "ASimple", indexes = @Index(
			name = "i_asimple_avalue_astringValue",
			columnList = "aValue ASC, aStringValue DESC"
	))
	public static class ASimpleFirst {
		@Id
		@GeneratedValue
		private Integer id;
		private Integer aValue;
		private String aStringValue;
		@ManyToOne(cascade = CascadeType.ALL)
		@JoinColumns(
				value = { @JoinColumn( name ="id1" ), @JoinColumn( name = "id2" ) },
				foreignKey = @ForeignKey( name = "fk_asimple_aother")
		)
		private AOther aOther;
	}

	@Entity(name = "ASimple")
	@Table(name = "ASimple", indexes = {
			@Index( name = "i_asimple_avalue_astringvalue", columnList = "aValue ASC, aStringValue DESC"),
			@Index( name = "i_asimple_avalue_data", columnList = "aValue DESC, data ASC")
	},
			uniqueConstraints = { @UniqueConstraint( name = "u_asimple_astringvalue", columnNames = "aStringValue")}
	)
	public static class ASimpleNext {
		@Id
		@GeneratedValue
		private Integer id;

		private Integer aValue;

		private String aStringValue;

		private String data;

		@ManyToOne(cascade = CascadeType.ALL)
		@JoinColumns(
				value = { @JoinColumn( name ="id1" ), @JoinColumn( name = "id2" ) },
				foreignKey = @ForeignKey( name = "fk_asimple_aother")
		)
		private AOther aOther;

		@ManyToOne(cascade = CascadeType.ALL)
		@JoinColumn(foreignKey = @ForeignKey( name = "fk_asimple_aanother"))
		private AAnother aAnother;
	}

	@Entity(name = "AOther")
	@IdClass(AOtherId.class)
	public static class AOther {
		@Id
		private int id1;

		@Id
		private String id2;

		private String anotherString;
	}

	public static class AOtherId implements Serializable {
		private int id1;
		private String id2;

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			AOtherId aOtherId = (AOtherId) o;
			return id1 == aOtherId.id1 && id2.equals( aOtherId.id2 );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id1, id2 );
		}
	}

	@Entity(name = "AAnother")
	public static class AAnother {
		@Id
		@GeneratedValue
		private Integer id;

		private String description;
	}
}
