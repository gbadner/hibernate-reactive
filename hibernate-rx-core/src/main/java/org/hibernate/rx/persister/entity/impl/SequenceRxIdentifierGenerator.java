package org.hibernate.rx.persister.entity.impl;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IntegralDataTypeHolder;
import org.hibernate.id.enhanced.AccessCallback;
import org.hibernate.id.enhanced.Optimizer;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.rx.impl.RxQueryExecutor;
import org.hibernate.rx.util.impl.RxUtil;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionStage;

/**
 * Support for JPA's {@link javax.persistence.SequenceGenerator}.
 */
public class SequenceRxIdentifierGenerator implements RxIdentifierGenerator<Long> {

	private static final RxQueryExecutor queryExecutor = new RxQueryExecutor();

	private final RxOptimizer optimizer;

	private final String sql;

	SequenceRxIdentifierGenerator(PersistentClass persistentClass, PersisterCreationContext creationContext, RxOptimizer optimizer) {

		this.optimizer = optimizer;

		MetadataImplementor metadata = creationContext.getMetadata();
		SessionFactoryImplementor sessionFactory = creationContext.getSessionFactory();
		Database database = metadata.getDatabase();
		JdbcEnvironment jdbcEnvironment = database.getJdbcEnvironment();

		Properties props = IdentifierGeneration.identifierGeneratorProperties(
				jdbcEnvironment.getDialect(),
				sessionFactory,
				persistentClass
		);

		QualifiedName logicalQualifiedSequenceName =
				IdentifierGeneration.determineSequenceName( props, jdbcEnvironment, database );
		final Namespace namespace = database.locateNamespace(
				logicalQualifiedSequenceName.getCatalogName(),
				logicalQualifiedSequenceName.getSchemaName()
		);
		org.hibernate.boot.model.relational.Sequence sequence =
				namespace.locateSequence( logicalQualifiedSequenceName.getObjectName() );

		if (sequence != null) {
			final Dialect dialect = jdbcEnvironment.getDialect();
			String finalSequenceName = jdbcEnvironment.getQualifiedObjectNameFormatter().format(
					sequence.getName(),
					dialect
			);
			sql = dialect.getSequenceNextValString(finalSequenceName);
		}
		else {
			sql = null;
		}
	}

	@Override
	public CompletionStage<Optional<Long>> generate(SharedSessionContractImplementor session) {
		return sql==null
				? RxUtil.completedFuture(Optional.empty())
				: optimizer.generate( new RxAccessCallback() {
					@Override
					public CompletionStage<Optional<Long>> getNextValue() {
						return queryExecutor.selectLong( sql, new Object[0], session.getFactory() );
					}

					@Override
					public String getTenantIdentifier() {
						return session.getTenantIdentifier();
					}
				}
		);
	}
}
