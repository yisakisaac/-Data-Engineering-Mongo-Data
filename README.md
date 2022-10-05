# Data-Engineering-Mongo-Data

Process, upload, and generate reporting data on users. 


1. Upload the user.csv file into MongoDB
2. Upload the new_user.csv file into MongoDB
   - assume existing records will match on street, city, state, and zipcode
3. Generate the following CSV files from the data in MongoDB. For all CSV files, output dates in UTC:
   - Find those that have responded in the first half of 2020, sort by response date
   - Responded unfavorably in September of 2020, sort by response date
   - Have not tried to contacted in 2020, sort by send date
   - Missing First Name, sort by last name
