# Implementation-of-DBMS
Implement a basic DBMS, including Record-Based File Manager, Relation Manager, Index Manager and Query Engine


1. Basic information
OS (bit) : Ubuntu
gcc version : 5.4.0


2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 
There are three kinds of page in the index file.
one is the directory page, which is used for indicating which page is the root page. They are also used in from the free pages in the file. One directory page will store 1024 pages' information. If there are more than 1024 pages, the 1025th page will become the now directory page, and so on.

ons is the overflow page, which is used to store the rid with the same key value. In the first cell of the page, there will be a pointer points to next overflow page if it exits. 

one is the node page, which can be divide into leaf node page and non-leaf node page


3. Index Entry Format
- Show your index entry design (structure). 
Generally, there are two kinds of entry formats, one is for non-leaf nodes another is for leaf nodes
The entry format is very similar to the record format. At first, there will be an indictor to store the information about the keyvalue, including the length of the keyvalue and kind of the key value. And it also includes the pointers, left node and right node, which are pointing to the next subtrees. Following the indicator, it is key value.

The entry format in the leaf node is similar to the entry format in non-leaf node. but there are some differences. The entry in the leaf node will include rid, which is the pointer towards the heap files. and it also includes the information about the overflow pages. There will be a pointer pointing the overflow page. The overflow page will store the rids with the same key values.


4. Page Format
- Show your internal-page (non-leaf node) design.
The internal-page design is very similar to the design of the record page. Entries will be stored at the beginning of the page. And in the last of the page, there will be a cell to store the information about the node, including the remaining size of the page, the type of key value. And there will be a mark to show if the node is an internal page or a leaf node page. 

- Show your leaf-page (leaf node) design.
The leaf-page design is similar to the design of internal-page node. It will store the entries at the beginning of the page. And at last of the page, there will be some bytes to store the information of the page, includingg the kind of the node(non-leaf or leaf) next point to sibling paga and previous point to previous page.

all of the data is stored in the format of struct.


5. Implementation Detail
- Have you added your own source file (.cc or .h)?
No, we do not add any personal source file, but we modify the given source files, adding some functions.

- Have you implemented non-lazy deletion? Choose Yes or No: 
Yes

- Have you implemented duplicated key handling that can span multiple pages? Choose Yes or No: 
  If yes, please explain briefly how you have implemented this feature.
Yes.
First we will check if the key value is already exist, if so, we will store the rid with the same key value to the overflow page.
Second we will find the free page( some deletion will generate free page) as the overflow page by scan the directory page.
Third, we will store the entry into the new overflow page, and update the entry'overflow page pointer in the leaf node. The overflow page also has a pointer to next overflow page which store the rid with the same key value.
General, the operations of the overflow pages are similar to operations of list.

- Other implementation details:


6. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)

referrence:
introduct to algorithms: page 488 -page 500, B tree
github: https://github.com/JZChen/CS222_DataBase/tree/master/ix, data structure and realzation
