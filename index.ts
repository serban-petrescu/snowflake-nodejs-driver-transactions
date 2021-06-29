import { Connection, createConnection } from 'snowflake-sdk';
import { config } from 'dotenv';

config();

async function connect(): Promise<Connection> {
    const connection = createConnection({
        account: process.env.SNOWFLAKE_ACCOUNT_NAME,
        username: process.env.SNOWFLAKE_USERNAME,
        password: process.env.SNOWFLAKE_PASSWORD,
        database: 'DEMO_DB',
        schema: 'PUBLIC',
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

async function runTest(connection: Connection): Promise<void> {
    await createTable(connection);
    try {
        await executeStatement(connection, 'INSERT INTO example VALUES (1);');
        await executeStatement(connection, 'INSERT INTO example VALUES (2);');
        await executeStatement(connection, 'COMMIT;');
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

    console.log('Running the test without autocommit.');
    await executeStatement(connection, 'ALTER SESSION SET AUTOCOMMIT = FALSE;');
    await runTest(connection);
}

main().then(() => console.log('Done'));
