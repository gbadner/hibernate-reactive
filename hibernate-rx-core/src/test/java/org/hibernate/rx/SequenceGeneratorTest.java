package org.hibernate.rx;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.id.enhanced.StandardOptimizerDescriptor;
import org.junit.Test;

import javax.persistence.*;
import java.util.Objects;

/**
 * Tests a sequence using the "no-op" optimizer.
 */
public class SequenceGeneratorTest extends BaseRxTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.setProperty(
				AvailableSettings.PREFERRED_POOLED_OPTIMIZER,
				StandardOptimizerDescriptor.NONE.getExternalName()
		);
		configuration.addAnnotatedClass( SequenceId.class );
		return configuration;
	}

	@Test
	public void testSequenceGenerator(TestContext context) {

		SequenceId b = new SequenceId();
		b.string = "Hello World";

		test( context,
				openSession()
				.thenCompose(s -> s.persist(b))
				.thenCompose(s -> s.flush())
				.thenCompose( v -> openSession())
				.thenCompose( s2 ->
					s2.find( SequenceId.class, b.getId() )
						.thenAccept( bt -> {
							context.assertTrue( bt.isPresent() );
							SequenceId bb = bt.get();
							context.assertEquals( bb.id, 5 );
							context.assertEquals( bb.string, b.string );
							context.assertEquals( bb.version, 0 );

							bb.string = "Goodbye";
						})
						.thenCompose(vv -> s2.flush())
						.thenCompose(vv -> s2.find( SequenceId.class, b.getId() ))
						.thenAccept( bt -> {
							context.assertEquals( bt.get().version, 1 );
						}))
				.thenCompose( v -> openSession())
				.thenCompose( s3 -> s3.find( SequenceId.class, b.getId() ) )
				.thenAccept( bt -> {
					SequenceId bb = bt.get();
					context.assertEquals(bb.version, 1);
					context.assertEquals( bb.string, "Goodbye");
				})
		);
	}

	@Entity
	@SequenceGenerator(name = "seq",
			sequenceName = "test_id_seq",
			initialValue = 5)
	public static class SequenceId {
		@Id @GeneratedValue(generator = "seq")
		Integer id;
		@Version Integer version;
		String string;

		public SequenceId() {
		}

		public SequenceId(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			SequenceId sequenceId = (SequenceId) o;
			return Objects.equals(string, sequenceId.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
