/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.cfg.NotYetImplementedException;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.tool.schema.extract.internal.AbstractInformationExtractorImpl;
import org.hibernate.tool.schema.extract.spi.ExtractionContext;

public class ReactiveInformationExtractorImpl extends AbstractInformationExtractorImpl {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( ReactiveInformationExtractorImpl.class );

	private final ExtractionContext extractionContext;

	public ReactiveInformationExtractorImpl(ExtractionContext extractionContext) {
		super( extractionContext );
		this.extractionContext = extractionContext;
	}

	@Override
	protected String getResultSetCatalogLabel() {
		return normalizeLabel( super.getResultSetCatalogLabel() );
	}
	@Override
	protected String getResultSetSchemaLabel() {
		return normalizeLabel( super.getResultSetSchemaLabel() );
	}
	@Override
	protected String getResultSetTableNameLabel() {
		return normalizeLabel( super.getResultSetTableNameLabel() );
	}
	@Override
	protected String getResultSetTableTypeLabel() {
		return normalizeLabel( super.getResultSetTableTypeLabel() );
	}
	@Override
	protected String getResultSetRemarksLabel() {
		return normalizeLabel( super.getResultSetRemarksLabel() );
	}
	@Override
	protected String getResultSetColumnNameLabel() {
		return normalizeLabel( super.getResultSetColumnNameLabel() );
	}

	@Override
	protected String getResultSetSqlTypeCodeLabel() {
		return normalizeLabel( super.getResultSetSqlTypeCodeLabel() );
	}

	@Override
	protected String getResultSetTypeNameLabel() {
		return normalizeLabel( super.getResultSetTypeNameLabel() );
	}

	@Override
	protected String getResultSetColumnSizeLabel() {
		return normalizeLabel( super.getResultSetColumnSizeLabel() );
	}

	@Override
	protected String getResultSetDecimalDigitsLabel() {
		return normalizeLabel( super.getResultSetDecimalDigitsLabel() );
	}

	@Override
	protected String getResultSetIsNullableLabel() {
		return normalizeLabel( super.getResultSetIsNullableLabel() );
	}

	@Override
	protected String getResultSetIndexTypeLabel() {
		return normalizeLabel( super.getResultSetIndexTypeLabel() );
	}

	@Override
	protected String getResultSetIndexNameLabel() {
		return normalizeLabel( super.getResultSetIndexNameLabel() );
	}

	@Override
	protected String getResultSetForeignKeyLabel() {
		return normalizeLabel( super.getResultSetForeignKeyLabel() );
	}

	@Override
	protected String getResultSetPrimaryKeyNameLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeyNameLabel() );
	}

	@Override
	protected String getResultSetColumnPositionColumn() {
		return normalizeLabel( super.getResultSetColumnPositionColumn() );
	}

	@Override
	protected String getResultSetPrimaryKeyColumnNameLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeyColumnNameLabel() );
	}

	@Override
	protected String getResultSetForeignKeyColumnNameLabel() {
		return normalizeLabel( super.getResultSetForeignKeyColumnNameLabel() );
	}

	@Override
	protected String getResultSetPrimaryKeyCatalogLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeyCatalogLabel() );
	}

	@Override
	protected String getResultSetPrimaryKeySchemaLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeySchemaLabel() );
	}

	@Override
	protected String getResultSetPrimaryKeyTableLabel() {
		return normalizeLabel( super.getResultSetPrimaryKeyTableLabel() );
	}

	private String normalizeLabel(String columnLabel) {
		return toMetaDataObjectName( Identifier.toIdentifier( columnLabel ) );
	}

	@Override
	protected <T> T processCatalogsResultSet(ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		return extractionContext.getQueryResults(
				String.format(
						"SELECT catalog_name AS %s FROM information_schema.information_schema_catalog_name",
						getResultSetCatalogLabel()
				),
				processor
		);
	}

	@Override
	protected <T> T processSchemaResultSet(
			String catalogFilter,
			String schemaFilter,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		final StringBuilder sb = new StringBuilder()
				.append( "SELECT catalog_name AS " ).append( getResultSetCatalogLabel() )
				.append( " , schema_name AS "  ).append( getResultSetSchemaLabel() )
				.append( " FROM information_schema.schemata WHERE true" );
		appendSingleQuotedEscapedStringIfNonNull( " AND catalog_name = ", catalogFilter, sb );
		appendSingleQuotedEscapedStringIfNonNull( " AND schema_name = ", schemaFilter, sb );
		return extractionContext.getQueryResults( sb.toString(), processor );
	}

	@Override
	protected <T> T processTableResultSet(
			String catalogFilter,
			String schemaFilter,
			String tableNameFilter,
			String[] tableTypes,
			ExtractionContext.ResultSetProcessor<T> processor
	) throws SQLException {

		final StringBuilder sb = new StringBuilder()
				.append( "SELECT table_catalog AS " ).append( getResultSetCatalogLabel() )
				.append( " , table_schema AS "  ).append( getResultSetSchemaLabel() )
				.append( " , table_name AS " ).append( getResultSetTableNameLabel() )
				.append( " , table_type AS " ).append( getResultSetTableTypeLabel() )
				.append( " , null AS " ).append( getResultSetRemarksLabel() )
					// Remarks are not available from information_schema.
					// Hibernate ORM does not currently do anything with remarks,
					// so just return null for now.
				.append( " FROM information_schema.tables WHERE true" );
		appendSingleQuotedEscapedStringIfNonNull( " AND table_catalog = ", catalogFilter, sb );
		appendSingleQuotedEscapedStringIfNonNull( " AND table_schema = ", schemaFilter, sb );
		appendSingleQuotedEscapedStringIfNonNull( " AND table_name = ", tableNameFilter, sb );

		if ( tableTypes != null && tableTypes.length > 0 ) {
			appendSingleQuotedEscapedStringIfNonNull(
					" AND table_type IN ( ",
					tableTypes[0].equals( "TABLE" ) ? getResultSetTableTypesPhysicalTableConstant() : tableTypes[0],
					sb
			);
			for ( int i = 1 ; i < tableTypes.length ; i++ ) {
				appendSingleQuotedEscapedStringIfNonNull(
						", ",
						tableTypes[i].equals( "TABLE" ) ? getResultSetTableTypesPhysicalTableConstant() : tableTypes[i],
						sb
				);
			}
			sb.append( " ) " );
		}
		return extractionContext.getQueryResults( sb.toString(), processor );
	}

	@Override
	protected String getResultSetTableTypesPhysicalTableConstant() {
		return "BASE TABLE";
	}

	@Override
	protected <T> T processColumnsResultSet(
			String catalogFilter,
			String schemaFilter,
			String tableName,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {

		final StringBuilder sb = new StringBuilder()
				.append( "SELECT table_name AS " ).append( getResultSetTableNameLabel() )
				.append( ", column_name AS " ).append( getResultSetColumnNameLabel() )
				.append( ", udt_name AS " ).append( getResultSetTypeNameLabel() )
				.append( ", null AS " ).append( getResultSetColumnSizeLabel() )
					// Column size is fairly complicated to get out of information_schema
					// and likely to be DB-dependent. Currently, Hibernate ORM does not use
					// column size for anything, so for now, just return null.
				.append( ", null AS " ) .append( getResultSetDecimalDigitsLabel() )
					// Decimal digits is fairly complicated to get out of information_schema
					// and likely to be DB-dependent. Currently, Hibernate ORM does not use
					// decimal digits for anything, so for now, just return null.
				.append( ", is_nullable AS " ).append( getResultSetIsNullableLabel() )
				.append( ", null AS " ).append( getResultSetSqlTypeCodeLabel() )
					// SQL type code is not available from information_schema,
					// and, for PostgreSQL at least, it appears to be hard-coded
					// into the JDBC driver. Currently, Hibernate ORM only uses
					// the SQL type code for SchemaMigrator to check if a column
					// type in the DB is consistent with what is computed in
					// Hibernate's metadata for the column. ORM also considers
					// the same column type name as a match, so the SQL code is
					// optional. For now, just return null for the SQL type code.
				.append( " FROM information_schema.columns WHERE true" );

		appendSingleQuotedEscapedStringIfNonNull( " AND table_catalog = " , catalogFilter, sb );
		appendSingleQuotedEscapedStringIfNonNull( " AND table_schema = " , schemaFilter, sb );
		appendSingleQuotedEscapedStringIfNonNull( " AND table_name = " , tableName, sb );

		sb.append(  "ORDER BY table_name, column_name, ordinal_position" );

		return extractionContext.getQueryResults( sb.toString(), processor );
	}

	@Override
	protected <T> T processPrimaryKeysResultSet(
			String catalogFilter,
			String schemaFilter,
			Identifier tableName,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {
		// This functionality is not used by ORM.
		throw new NotYetImplementedException();
	}

	@Override
	protected <T> T processIndexInfoResultSet(
			String catalogFilter,
			String schemaFilter,
			Identifier tableName,
			boolean approximate,
			ExtractionContext.ResultSetProcessor<T> processor) throws SQLException {

		// Generate the inner query first.
		final StringBuilder innerQuery = new StringBuilder()
				.append( "SELECT ci.relname AS index_name" )
				.append( " , CASE i.indisclustered WHEN true THEN " ).append( DatabaseMetaData.tableIndexClustered )
				.append( " ELSE CASE am.amname WHEN 'hash' THEN " ).append( DatabaseMetaData.tableIndexHashed )
				.append( " ELSE " ).append( DatabaseMetaData.tableIndexOther ).append( " END" )
				.append( " END AS index_type" )
				.append( " , (information_schema._pg_expandarray(i.indkey)).n AS position" )
				.append( " , ci.oid AS ci_iod" )
				.append( " FROM pg_catalog.pg_class ct" )
				.append( " JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid)" )
 				.append( " JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid)" )
				.append( " JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid)" )
				.append( " JOIN pg_catalog.pg_am am ON (ci.relam = am.oid)" )
				.append( " WHERE true" );

		appendSingleQuotedEscapedStringIfNonNull( " AND n.nspname = ", schemaFilter, innerQuery );
		appendSingleQuotedEscapedStringIfNonNull( " AND ct.relname = ", tableName.getText(), innerQuery );

		return extractionContext.getQueryResults(
				"SELECT tmp.index_name AS " + getResultSetIndexNameLabel() +
						", tmp.index_type AS " + getResultSetIndexTypeLabel() +
						", trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.ci_iod, tmp.position, false)) AS " + getResultSetColumnNameLabel() +
						" FROM ( " + innerQuery + " ) tmp" +
						" ORDER BY " + getResultSetIndexNameLabel() + ", tmp.position",
				processor
		);
	}

	@Override
	protected <T> T processImportedKeysResultSet(
			String catalogFilter,
			String schemaFilter,
			String tableName,
			ExtractionContext.ResultSetProcessor<T> processor
	) throws SQLException {
		final StringBuilder sb = new StringBuilder()
				.append( "SELECT NULL AS " ).append( getResultSetPrimaryKeyCatalogLabel() )
				.append( ", pkn.nspname AS " ).append( getResultSetPrimaryKeySchemaLabel() )
				.append( ", pkc.relname AS " ).append( getResultSetPrimaryKeyTableLabel() )
				.append( ", pka.attname AS " ).append( getResultSetPrimaryKeyColumnNameLabel() )
				.append( ", fka.attname AS " ).append( getResultSetForeignKeyColumnNameLabel() )
				.append( ", pos.n AS " ).append( getResultSetColumnPositionColumn() )
				.append( ", con.conname AS " ).append( getResultSetForeignKeyLabel() )
				.append( " FROM pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka" )
				.append( ",  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka" )
				.append( ", pg_catalog.pg_constraint con" )
				.append( ", pg_catalog.generate_series(1, CAST( (SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys') AS INTEGER ) ) pos(n)" )
				.append( " WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid" )
				.append( " AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid" )
				.append( " AND con.contype = 'f' " );

				appendSingleQuotedEscapedStringIfNonNull( " AND fkn.nspname = ", schemaFilter, sb );
				appendSingleQuotedEscapedStringIfNonNull( " AND fkc.relname = ", tableName, sb );

				sb.append( " ORDER BY pkn.nspname,pkc.relname, con.conname, pos.n");

		return extractionContext.getQueryResults( sb.toString(), processor );
	}

	private void appendSingleQuotedEscapedStringIfNonNull(
			String prefix,
			String value,
			StringBuilder sb) {

		if ( value != null ) {
			// TODO: make this work properly with values that contain a quote
			sb.append( prefix )
					.append( '\'' )
					.append( value )
					.append( '\'' );
		}
	}
}
