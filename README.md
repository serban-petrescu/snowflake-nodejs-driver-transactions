## Snowflake transactions with the NodeJS driver
A very small PoC that shows how transactions can be used with Snowflake's NodeJS driver.

### Setting it up
 - You need to have NodeJS installed locally. 
 - Clone the repository and do a `npm ci` in the root folder.
 - Create a `.env` file in the root folder with the following contents (adjust the values accordingly):

```properties
SNOWFLAKE_ACCOUNT=abc1234.us-east-1
SNOWFLAKE_USERNAME=USERNAME
SNOWFLAKE_PASSWORD=PASSWORD

# Optional - only if you want to try out the export as well:
S3_LOCATION=s3://bucket/prefix/
S3_AWS_ACCESS_KEY_ID=KEY
S3_AWS_SECRET_ACCESS_KEY=SECRET
S3_AWS_SESSION_TOKEN=TOKEN (obtained via aws sts get-session-token)

```
 - Run `npm start`.