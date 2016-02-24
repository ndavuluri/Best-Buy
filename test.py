import xml.etree.ElementTree as ET, os, glob
os.chdir("/home/samantha/ds/product_data/reviews")
#os.chdir("/home/samantha/ds/tmp/rev")
'''filename_arr={}
i=0
for files in glob.glob("*.xml"):
    filename_arr[i] = files
    i= i+1

for key,value in filename_arr.items():
    print key , value

def write_report(r, filename):    
    with open(filename, "a") as input_file:
        for k, v in r.items():
            line = '{}, {}'.format(k, v) 
            print(line, file=input_file)
def write_report(r, filename):
        input_filename=open(filename, "a")
        input_filename.close()
        for (k,v) in r.items():
               input_filename.write(k,v)
        return filename
'''
'''
D = {}
mylist = []
x = "review1"
mylist.append(x)
D["a"] = mylist
if "a" in D.keys():
	y = "review2"
	mylist.append(y)
print D["a"] 
'''
# dups - 24122,24145,24165
#Wrong as prodid is not uniq , reviews can have same prod id
D = {}
file = open("/home/samantha/ds/tmp/rev/test", 'a')
for files in glob.glob("reviews_00*.xml"):
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
			print("duplicate")
			mylist = mylist + D[a]
			#mylist.append(D[a])
		D[a] = mylist
		#print type(D[a])
		#print D[a]
		
def write_report(r, filename):    
    with open(filename, "a") as input_file:
        for k, v in r.items():
            line = '{}, {}'.format(k, v)
            #print(line, file = input_file)
	    input_file.write(line)
	    input_file.write("\n")
#write_report(E,"/home/samantha/ds/tmp/rev/test")
write_report(D, "/home/samantha/ds/tmp/rev/Summary")
