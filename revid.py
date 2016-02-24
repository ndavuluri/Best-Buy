import xml.etree.ElementTree as ET, os, glob
os.chdir("/home/samantha/ds/product_data/reviews")
#os.chdir("/home/samantha/ds/tmp/rev")

D = {}
file = open("/home/samantha/ds/tmp/prodrev_result/id_review", 'a')
for files in glob.glob("reviews_00*.xml"):
	tree = ET.parse(files)
	root = tree.getroot()
	for child in root:
		mylist = []
		for x in child:
			if x.tag == "sku":
				a = x.text;
				file.write(a + '\n') 

def write_report(r, filename):    
    with open(filename, "a") as input_file:
        for k, v in r.items():
            line = '{}, {}'.format(k, v)
            #print(line, file = input_file)
	    input_file.write(line)
	    input_file.write("\n")
write_report(D, "/home/samantha/ds/tmp/rev/Summary")
