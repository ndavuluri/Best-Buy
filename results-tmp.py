import xml.etree.ElementTree as ET, os, glob
os.chdir("/home/samantha/ds/tmp/prodrev")

def write_report(r, filename):    
    with open(filename, "a") as input_file:
        for k, v in r.items():
            line = '{}, {}'.format(k, v) 
	    input_file.write(line)
	    input_file.write("\n")
C = {}
for files in glob.glob("products_*.xml"):
	tree = ET.parse(files)
	root = tree.getroot()
	for child in root:
		mylist = []
		for x in child:
			if x.tag == "productId":
				a = x.text
			if x.tag == "name":
				mylist.append(x.text);
			if x.tag == "type":
				mylist.append(x.text);
		
		C[a] = mylist

D = {}
file = open("/home/samantha/ds/tmp/prodrev/id", 'a')
for files in glob.glob("reviews_*.xml"):
	tree = ET.parse(files)
	root = tree.getroot()
	for child in root:
		mylist = []
		duplist = []
		for x in child:
			if x.tag == "id":
				a = x.text;
				file.write(a + '\n') 
			if x.tag == "comment":
				mylist.append(x.text)
		if a in D.keys():
			mylist = mylist + D[a]
		D[a] = mylist
consolidated = {}
mylist = []
for key in D.keys():
 if key in C.keys():
  	mylist.append(C[key]);
	mylist.append(D[key])
	consolidated[key] = mylist
        mylist = []
write_report(C, "/home/samantha/ds/tmp/prodrev_result/product")
write_report(D, "/home/samantha/ds/tmp/prodrev_result/review")
write_report(consolidated, "/home/samantha/ds/tmp/prodrev_result/clubbed")


