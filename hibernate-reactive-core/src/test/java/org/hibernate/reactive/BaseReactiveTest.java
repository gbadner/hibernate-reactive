/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;


import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.stage.Stage;
import org.hibernate.reactive.testing.SessionFactoryManager;
import org.hibernate.reactive.vertx.VertxInstance;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.runner.RunWith;

import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;


/**
 * Base class for unit tests that need a connection to the selected db and
 * need to wait for the end of the task for the assertion.
 * <p>
 *     Uses the {@link RunTestOnContext} rule to guarantee that all tests
 *     will run in the Vert.x event loop thread (by default, the tests using Vert.x unit will start
 *     in the JUnit thread).
 * </p>
 * <p>
 *     Contains several utility methods to make it easier to test Hibernate Reactive
 *     using Vert.x unit.
 * </p>
 */
@RunWith(VertxUnitRunner.class)
public abstract class BaseReactiveTest extends AbstractReactiveTest {

	public static SessionFactoryManager factoryManager = new SessionFactoryManager();

	@Before
	public void before(TestContext context) {
		Async async = context.async();
		vertxContextRule.vertx()
				.executeBlocking(
						// schema generation is a blocking operation and so it causes an
						// exception when run on the Vert.x event loop. So call it using
						// Vertx.executeBlocking()
						this::startFactoryManager,
						event -> {
							if ( event.succeeded() ) {
								async.complete();
							}
							else {
								context.fail( event.cause() );
							}
						}
				);
	}

	private void startFactoryManager(Promise<Object> p) {
		try {
			factoryManager.start( this::createHibernateSessionFactory );
			p.complete();
		}
		catch (Throwable e) {
			p.fail( e );
		}
	}

	private SessionFactory createHibernateSessionFactory() {
		Configuration configuration = constructConfiguration();
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.addService( VertxInstance.class, (VertxInstance) () -> vertxContextRule.vertx() )
				.applySettings( configuration.getProperties() );
		addServices( builder );
		StandardServiceRegistry registry = builder.build();
		configureServices( registry );
		return configuration.buildSessionFactory( registry );
	}

	protected SessionFactoryManager getSessionFactoryManager() {
		return factoryManager;
	}

	protected static Stage.SessionFactory staticSessionFactory() {
		return factoryManager.getHibernateSessionFactory().unwrap( Stage.SessionFactory.class );
	}

	@AfterClass
	public static void closeFactory(TestContext context) {
		test( context, factoryManager.stop() );
	}
}
