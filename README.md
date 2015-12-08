# DataGenerator
Generates scripts to insert dummy data for testing.

Poor man's version of Redgate's awesome tool: https://www.red-gate.com/products/sql-development/sql-data-generator/

Assumes that the first column is an identity.

Steps:
* Reads list of tables from the TablesToPopulate.supersecret file in the bin\Debug\Inputs folder.
    * Provide the schema name and table name, separated by a period.
* Gets the columns in each table.
* Generates the script to insert each record.
    * Doesn't consider constraints like foreign keys.
* Outputs the scripts to one sql file per table in the bin\Debug\Outputs folder.
 
