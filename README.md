# Pipeline_Stripe_MongoDB
Functional Pipeline to get Stripe Tables Cleaned and formatted into MongoDB for warehousing

This pipeline consists of multiple .py files for fine tuning and troubleshooting. 

# Pipeline process

1. A connection is verified with MongoDb server and Stripe before proceeding. There are two Stripe payment database that needs to be joined for our final analysis
  
2. The source data is imported through an API connection to the Stripe servers with pagination to ensure that the right data is imported. 
   
3. Selected tables are dumped into a loading zone in a clustered MongoDB database to avoid Stripe API call limits.

4. The tables in the landing zone are imported from the MongoDB database to be normalised and cleaned. The subscription table was joined with the customer table to get the customer information.

5. The manipulated data is then exported back into a curated data zone in the MongoDB database ready to be analysed

6. A pipeline file is created to automatically run all individual pipeline processes and the schedule is set in the Digital Ocean scheduler. 

7. An environment variable file '.env' is used to store sensitive API keys and other login information.


