<hr>

You need to download **2** items before can proceed with scala development on Cloudera. Since the Cloudera system is using Java SE 7 and the Eclipse is installed with 2015 version, we have to download compatible Spark jar files and the IDE software for that particular version.

## 1. Spark-core Jar Files

a. Download spark-core_2.11 from group org.apache.spark **(version 1.6.0)** with the link below https://jar-download.com/artifacts/org.apache.spark/spark-core_2.11?p=3

-- *Last option on page 3*

![image](https://github.com/ryoshi007/BDAGroupAss/blob/main/Instruction_Picture/Pasted%20image%2020231220005326.png?raw=true)

b. After the file is downloaded, extracted the contents (all jar files) to the shared folder with Cloudera.

![[Pasted image 20231220005546.png]]

c. Create a folder in Cloudera. And then move all the jar files from the shared folder (like in this case - pc-connector) to the newly created folder (spark-core). Remember the location of the folder that contains jar files as it will be added to IDE later during development.

![[Pasted image 20231220005836.png]]


## 2. Scala IDE

The functionality of original Eclipse software in Cloudera can be extended to program using Scala language.

a. Download zip file of 4.2.0 Release for Scala 2.11.7 with the link below. You can download on Cloudera system natively with Mozilla Firefox or through Windows system and drag the file to Cloudera using shared folder.
https://scala-ide.org/download/prev-stable.html

![[Pasted image 20231220010201.png]]

b. In the Downloads folder (if you download in Cloudera), extract the zip file by double clicking it. You can put the extracted file in any folder you like (as long as you remember its location). In the end, you will get a new folder called **"site"*.

![[Pasted image 20231220010624.png]]

c. Open Eclipse. Go to Help > Install New Software...

![[Pasted image 20231220010722.png]]

d. Select "Add" and provide the details like picture below

	Name: Scala IDE for Scala 4.2

Then select Local... and find the "site" folder than extracted just now, then click Ok

![[Pasted image 20231220011024.png]]

After you clicked "Ok", it should look like this.

![[Pasted image 20231220011038.png]]

e. Select `Scala IDE for Eclipse` and then click "Next", and then "Next" again.

(Mine grey out because I have installed it already)

![[Pasted image 20231220011401.png]]

f. Select `accept terms of license agreements` and then "Finish"

![[Pasted image 20231220011512.png]]

g. If it asks you to restart Eclipse IDE, press "Restart Now". (If it didn't ask then don't worry about it)

![[Pasted image 20231220011701.png]]

h. Click the "Open Perspective" icon (it looks like table with a + icon on top right). Select Scala > Ok.

![[Pasted image 20231220011853.png]]

i. Now you can start building Scala project.

![[Pasted image 20231220011945.png]]

j. Scala project configuration. Just put a project name, then select "Next >".

![[Pasted image 20231220012017.png]]

Select Libraries > Add External JARs.... Now you need to select all Jar files that we downloaded before in Step 1.

![[Pasted image 20231220012058.png]]

After that, select "Finish" and now you can build a Scala project without any issue.

![[Pasted image 20231220012300.png]]

k. Start the main program by creating new **Scala Object**. Enjoy!!

![[Pasted image 20231220012324.png]]
