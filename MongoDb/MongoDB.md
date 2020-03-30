3. Insert all the records from the given file into the **users** collection. 

   1) Query: db.users.insertMany(*All the records*)

   2) Result: <img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206030923899.png" alt="image-20191206030923899" style="zoom:35%;" />

4. Retrieve all the users sorted by name. 

   1) Query: db.users.find().sort({"Name":1})

   2) Result: <img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206031121616.png" alt="image-20191206031121616" style="zoom:35%;" />

5. List *only the id and names* sorted in reverse alphabetical order by name (Z-to-A). 

   1) Query: db.users.find({}, {Name:1, _id:1}).sort({Name:-1})

   2) Result:<img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206031335815.png" alt="image-20191206031335815" style="zoom:35%;" />

6. Is the attribute name case-sensitive? Try with the previous query. 

   1) Query: db.users.find({}, {Name:1, _id:1}).sort({NAME:1}) ; db.users.find({}, {NAME:1, _id:1}).sort({NAME:1})

   2) Result: We can see from the result that the attribute name is case-sensitive.

   <img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206031508920.png" alt="image-20191206031508920" style="zoom:35%;" /><img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206031541624.png" alt="image-20191206031541624" style="zoom:35%;" />

7. Repeat #5 but do not show the _id field. 

   1) Query: db.users.find({}, {Name:1, _id:0}).sort({Name:-1})

   2) Result: <img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206031750647.png" alt="image-20191206031750647" style="zoom:35%;" />

8. Insert the following document to the collection. {Name: {First: “David”, Last: “Bark”}} 

   1) Query: db.users.insertOne({Name:{First:"David",Last:"Bark"}})

   2) Result: ![image-20191206031914182](/Users/buming/Library/Application Support/typora-user-images/image-20191206031914182.png)

9. Rerun query #5. Where do you expect the new record to be located in the sort order? Verify the answer and explain. 

   1) Answer: The new record should be the first one. Because the ascii of the punctuation "{" is the largest.

   2) Result: <img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206032316022.png" alt="image-20191206032316022" style="zoom:35%;" />

10. Insert the following object into the users collection. {Name: [“David”, “Bark”]} 

    1) Query: db.users.insert({Name:["David","Bark"]})

    2) Result: ![image-20191206032856978](/Users/buming/Library/Application Support/typora-user-images/image-20191206032856978.png)

11. Repeat #5. Where do you expect the new object to appear in the sort order. Verify your answer and explain after running the query. 

    1) Answer: The result should be the second, since the "[" is the second largest.

    2) Result: <img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206034024174.png" alt="image-20191206034024174" style="zoom:35%;" />

    ​                I am so sorry about that, after two days of thinking this problem, I still couldn't figure out why this is not in the second position. I used to think about the objectid, however the answer is still wrong if sorted by objectid.

12. Repeat #5 again but this time sort the name in ascending order. Where do you expect the last inserted record to appear this time? Does it appear in the same position? Explain why or why not. 

    1) Answer: The result should be the last second, since the "[" is the second largest.

    2) Result: <img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206034106344.png" alt="image-20191206034106344" style="zoom:35%;" />

    ​                 Still sorry that I couldn't figure this problem out.

13. Build an index on the Name field for the users collection. Is MongoDB able to build the index on that field with the different value types stored in the Name field? 

    1) Query: db.users.createIndex({Name:1}). Yes, MongoDB can build the index on the field with different value types.

    2) Result: <img src="/Users/buming/Library/Application Support/typora-user-images/image-20191206034950699.png" alt="image-20191206034950699" style="zoom:35%;" />