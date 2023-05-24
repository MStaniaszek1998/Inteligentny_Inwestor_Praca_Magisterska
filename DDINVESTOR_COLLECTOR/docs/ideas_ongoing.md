## Data-Driven Investor Ideas while writting in Ubuntu
1. How to mimic behaviour of Haddoop? 
   - Either create a new user (both linux and ubuntu) and only he will have access to DataLake folder
   - Play Around with chattr +i -i etc.
   
2. How to create archivization depends strictly on the mimic-Hadoop: Whether only this user will be able to archive or not<br>
   or  there is another way?
3. How to set up databases(Relational OLTP, Document etc.) on linux?
    We want to use docker, but docker must have a "proxy" to write the files into our disk
4. think About Selenium grid to load off the network requests, maybe avoid getting banned(?)
5. Add scrapping for BusinessInsider and Nasdaq for news or CompanyProfile (Depends on the time and ease of scrapping)
# REMEMBER TO PUT THIS INTO PRESENTATION^^^
### Duplicates Identification 
How to detect duplicates in the incoming data batches. <br> 
^^^^ Spark preprocessing or earlier? 


Javascript code to get any twitter href<br>
````javascript
document.querySelectorAll("a[href*='twitter']")[0].href
````
### To do list
1. Create a table in postgresql about company information or on mongo db?
2. Create a collection of tweets and business insider's headlines in mongodb
3. Create a Twitter scraper for the companies using companies_twitter.json 
- Done = cleaned up the folder structure and code for scrapping metadata
