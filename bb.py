import xml.etree.ElementTree as ET, os, glob
#import random
os.chdir("/home/samantha/ds/product_data/products")
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
def write_report(r, filename):    
    with open(filename, "a") as input_file:
        for k, v in r.items():
            line = '{}, {}'.format(k, v) 
            #print(line, file = input_file)
	    input_file.write(line)
	    input_file.write("\n")
D = {}
for files in glob.glob("*.xml"):
	tree = ET.parse(files)
	root = tree.getroot()
	for child in root:
		mylist = []
		for x in child:
			if x.tag == "productId":
				a = x.text
#				rand = random.random()
#				b = a + '_' + str(rand)
			if x.tag == "name":
				mylist.append(x.text);
			if x.tag == "type":
				mylist.append(x.text);
		
		D[a] = mylist
write_report(D, "/home/samantha/ds/Results/prod_summary")
#		print a, D[a]

	
