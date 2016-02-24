import xml.etree.ElementTree as ET, os, glob
os.chdir("/home/samantha/ds/product_data/products")

D = {}
file = open("/home/samantha/ds/Results/type", "a")
for files in glob.glob("*.xml"):
	tree = ET.parse(files)
	root = tree.getroot()
	for child in root:
		mylist = []
		for x in child:
			if x.tag == "type":
				a = x.text;
				file.write(a + '\n')
				
#		D[a] = mylist
def write_report(r, filename):    
    with open(filename, "a") as input_file:
        for k, v in r.items():
            line = '{}, {}'.format(k, v) 
	    input_file.write(line + '\n')
#write_report(D, "/home/samantha/ds/tmp/rev/Summary.txt")
