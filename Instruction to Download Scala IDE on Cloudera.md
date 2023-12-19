![image](https://github.com/ryoshi007/BDAGroupAss/assets/74929046/e71c94cf-fcd6-40b1-9884-252f1c4b4089)<hr>

You need to download **2** items before can proceed with scala development on Cloudera. Since the Cloudera system is using Java SE 7 and the Eclipse is installed with 2015 version, we have to download compatible Spark jar files and the IDE software for that particular version.

## 1. Spark-core Jar Files

a. Download spark-core_2.11 from group org.apache.spark **(version 1.6.0)** with the link below https://jar-download.com/artifacts/org.apache.spark/spark-core_2.11?p=3

-- *Last option on page 3*

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220005326.png?raw=true)

b. After the file is downloaded, extract the contents (all jar files) to the shared folder with Cloudera.

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220005546.png?raw=true)

c. Create a folder in Cloudera. And then move all the jar files from the shared folder (like in this case - pc-connector) to the newly created folder (spark-core). Remember the location of the folder that contains jar files as it will be added to IDE later during development.

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220005836.png?raw=true)


## 2. Scala IDE

The functionality of the original Eclipse software in Cloudera can be extended to code using Scala language.

a. Download the zip file of 4.2.0 Release for Scala 2.11.7 with the link below. You can download it on the Cloudera system natively with Mozilla Firefox or through the Windows system and drag the file to Cloudera using a shared folder.
https://scala-ide.org/download/prev-stable.html

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220010201.png?raw=true)

b. In the Downloads folder (if you download in Cloudera), extract the zip file by double-clicking it. You can put the extracted file in any folder you like (as long as you remember its location). In the end, you will get a new folder called **"site"*.

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220010624.png?raw=true)

c. Open Eclipse. Go to Help > Install New Software...

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220010722.png?raw=true)

d. Select "Add" and provide the details like the picture below

	Name: Scala IDE for Scala 4.2

Then select Local... and find the "site" folder that was extracted just now, then click Ok

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220011024.png?raw=true)

After you click "Ok", it should look like this.

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220011038.png?raw=true)

e. Select `Scala IDE for Eclipse` and then click "Next", and then "Next" again.

(Mine grey out because I have installed it already)

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220011401.png?raw=true)


f. Select `accept terms of license agreements` and then "Finish"

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220011512.png?raw=true)

g. If it asks you to restart Eclipse IDE, press "Restart Now". (If it didn't ask then don't worry about it)

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220011701.png?raw=true)

h. Click the "Open Perspective" icon (it looks like a table with a + icon on top right). Select Scala > Ok.

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220011853.png?raw=true)

i. Now you can start building a Scala project.

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220011945.png?raw=true)

j. Scala project configuration. Just put a project name, then select "Next >".

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220012017.png?raw=true)

Select Libraries > Add External JARs.... Now you need to select all Jar files that we downloaded before in Step 1.

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220012058.png?raw=true)

After that, select "Finish" and now you can build a Scala project without any issues.

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220012300.png?raw=true)

k. Start the main program by creating a new **Scala Object**. Enjoy!!

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220012324.png?raw=true)
