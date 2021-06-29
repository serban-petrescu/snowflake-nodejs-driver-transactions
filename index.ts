import { Connection, createConnection } from 'snowflake-sdk';
import { config } from 'dotenv';

config();

async function connect(database = 'DEMO_DB', schema = 'PUBLIC'): Promise<Connection> {
    const connection = createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        database,
        schema,
    });

    return await new Promise((resolve, reject) =>
        connection.connect((err, conn) => (err ? reject(err) : resolve(conn))),
    );
}

function executeStatement(connection: Connection, sql: string): Promise<void> {
    return new Promise((resolve, reject) =>
        connection.execute({
            sqlText: sql,
            complete: (err) => (err ? reject(err) : resolve()),
        }),
    );
}

function getCount(connection: Connection): Promise<number> {
    return new Promise((resolve, reject) =>
        connection.execute({
            sqlText: 'SELECT COUNT(*) FROM example',
            complete: (err, _, rows) => (err ? reject(err) : resolve(rows[0]['COUNT(*)'])),
        }),
    );
}

async function createTable(connection: Connection): Promise<void> {
    await executeStatement(connection, 'CREATE TABLE example ( id integer );');
}

async function dropTable(connection: Connection): Promise<void> {
    await executeStatement(connection, 'DROP TABLE example;');
}

async function runTest(connection: Connection, begin?: boolean): Promise<void> {
    await createTable(connection);
    try {
        begin && (await executeStatement(connection, 'BEGIN;'));
        await executeStatement(connection, 'INSERT INTO example VALUES (1);');
        await executeStatement(connection, 'INSERT INTO example VALUES (2);');
        await executeStatement(connection, 'COMMIT;');
        begin && (await executeStatement(connection, 'BEGIN;'));
        await executeStatement(connection, 'INSERT INTO example VALUES (3);');
        await executeStatement(connection, 'INSERT INTO example VALUES (4);');
        await executeStatement(connection, "INSERT INTO example VALUES ('ASD');");
        await executeStatement(connection, 'COMMIT;');
    } catch (e) {
        await executeStatement(connection, 'ROLLBACK;');
    }
    const count = await getCount(connection);
    console.log(
        count === 0
            ? 'The table is empty, thus no inserts worked at all.'
            : count === 2
            ? 'There are 2 rows in the table, thus transactions work as expected.'
            : `There are ${count} rows in the table, thus transactions don't work.`,
    );
    await dropTable(connection);
}

async function main() {
    const connection = await connect();
    console.log('Running the test with the default connection.');
    await runTest(connection);
    console.log('');

    console.log('Running the test with the default connection and BEGINs.');
    await runTest(connection, true);
    console.log('');

    console.log('Running the test without autocommit or BEGINs.');
    await executeStatement(connection, 'ALTER SESSION SET AUTOCOMMIT = FALSE;');
    await runTest(connection);
    console.log('');

    if (process.env.S3_LOCATION) {
        const sample = await connect('SNOWFLAKE_SAMPLE_DATA', 'TPCH_SF100');

        console.log('Running export.');
        await executeStatement(sample, `
            COPY INTO '${process.env.S3_LOCATION}' FROM "CUSTOMER"
            credentials = (
                aws_key_id='${process.env.S3_AWS_ACCESS_KEY_ID}' 
                aws_secret_key='${process.env.S3_AWS_SECRET_ACCESS_KEY}' 
                aws_token='${process.env.S3_AWS_SESSION_TOKEN}'
            )
            file_format = (type = csv);
        `);
        console.log('');
    }
}

main().then(() => console.log('Done'));
