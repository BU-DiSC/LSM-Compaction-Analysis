#dimitris 10/10/2020
#------------------------------------------------------------#
import re

filePath = "file_to_parse.txt"

def bring_workload_parameters_to_the_top(values):
    new_values_list = list()
    for i in range(11):
        new_values_list.extend([values[i]])
    for i in range (568, 612):
        new_values_list.extend([values[i]])
    for i in range (11, 568):
        new_values_list.extend([values[i]])
    return new_values_list

def sum_list(line):
    sum = 0
    list = line.split("#")
    #print list
    #print "lista"
    for item in list:
        sum += float(item)
    #print sum
    return str(sum)

def get_values_from_special_parameters(sp):
    #print 'now working on:'
    #print sp
    number_of_levels = sp.count('@')
    for i in range(number_of_levels):
        #print ('@level' +  str(i+1) + ', to remove')
        sp = sp.replace('@level' +  str(i+1), ' ')
    sp = re.sub('[^0-9]',' ', sp)
    temp_list = sp.split()
    if len(temp_list) == 0:
        return "*" ##nothing

    values = ""
    for value in temp_list:
        values += value + '#'
    values = values.rstrip('#')
    #print values
    #print "PANW"
    return values

def get_first_word(seq):
    seq = seq.lstrip()
    return seq.split(' ', 1)[0]

def write_list_to_file(myList):
    with open('experiment_results.csv', 'a') as f:
        for item in myList:
            f.write(item + ';')
        f.write('\n')
    
    with open('output.csv', 'a') as f:
        for item in myList:
            f.write(item + ';')
        f.write('\n')

values = list()
myFile = open(filePath, 'r') 
Lines = myFile.readlines() 
prev_line_first_word = ""
for line in Lines: 
    special_part_of_line = '#'
    if prev_line_first_word == "cmpt_sty" or prev_line_first_word == "#p_ts_in_tree" or prev_line_first_word == "%space_amp": #if previous line begins with one of these
    #if False:    
        temp_list = line.split()
        values.extend(temp_list)

    current_line_first_word = get_first_word(line)
    # if current_line_first_word == "user_key_comparison_count" or current_line_first_word == "thread_pool_id" or ('rocksdb.' in current_line_first_word) or current_line_first_word == "files": #if current line begins with one of these
    #if current_line_first_word == "user_key_comparison_count":
        #remove characters and put all numbers in a list
    if 'rocksdb.' in current_line_first_word:
        line = line.replace("P50", "")
        line = line.replace('P95', "")
        line = line.replace("P99", "")
        line = line.replace("P100", "")
        line = line.replace("l0", "")
        line = line.replace("l1", "")
        line = line.replace("l2", "")
        temp_removed_characters = re.findall(r"[-+]?\d*\.\d+|\d+", line)
        #temp_removed_characters = re.sub('[^0-9]',' ', line)
        #temp_list = temp_removed_characters.split()
        values.extend(temp_removed_characters)

    if current_line_first_word == "user_key_comparison_count":
        #last parameters special save
        non_special_part_of_line = line.split("bloom_filter_useful",1)[0]
        #print non_special_part_of_line
        temp_removed_characters = re.sub('[^0-9]',' ', non_special_part_of_line)
        temp_list = temp_removed_characters.split()
        values.extend(temp_list)
        #print "#####################"
        special_part_of_line = line.split("bloom_filter_useful",1)[1]
        #print special_part_of_line
        special_parameters = special_part_of_line.split("bl")
        #print '-----------------EDWWW'
        #print special_parameters
        #print '----------------------------------'
        for sp in special_parameters:
            #values.extend(['-99']) # to make it easier to tell the difference
            values_from_special_parameters = get_values_from_special_parameters(sp)
            if values_from_special_parameters == "*":
                values.extend(["-1"])
            else:
                values.extend([values_from_special_parameters])
                #values.extend(sum_list(values_from_special_parameters))
        #print special_part_of_line
        #print special_parameters
    elif current_line_first_word == "thread_pool_id" or current_line_first_word == "files" or current_line_first_word == "Workload_parameters:":
        temp_removed_characters = re.findall(r"[-+]?\d*\.\d+|\d+", line)
        #temp_removed_characters = re.sub('[^0-9]',' ', line)
        #temp_list = temp_removed_characters.split()
        values.extend(temp_removed_characters)
        
    prev_line_first_word = current_line_first_word
values = bring_workload_parameters_to_the_top(values)
# print(values)
# print("----------------\nTotal number of values: %d" %len(values))
write_list_to_file(values)


