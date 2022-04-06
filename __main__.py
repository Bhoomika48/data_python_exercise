'''
The objective of this application is to reads 2 files
    1. student.csv with delimiter as '_'
    2. teachers.parquet file
Process the information in both files
Apply any transformations if required
Create an output file that shows all students, their teachers and registered classes
'''
import pandas
'''
This method reads and processes the student file 
It converts the file into a dataframe 
It adds a prefix to all column names
It adds a column to the dataframe to join both first and last name of student
Returns the transformed dataframe
'''
def processStudent():
    #Read students.csv file
    with open('data_engineer/students.csv') as students:
        studentsdf = pandas.read_csv(students, delimiter ='_')
        #add a prefix to all columnNames in students dataframe
    studentsdf = studentsdf.add_prefix('student')
    #Adding a new column studentName by combining studentfname and studentlname
    studentsdf['studentname'] = studentsdf['studentfname']+' '+studentsdf['studentlname']
    print(studentsdf)
    students.close()
    return studentsdf

'''
This method reads and processes the teachers file
It converts the file into a dataframe 
It adds a prefix to all column names
It adds a column to the dataframe to join both first and last name of teacher
Returns the transformed dataframe
'''
def processTeachers():
    #Read teachers.parquet file
    teachersdf = pandas.read_parquet('data_engineer/teachers.parquet')
    #add a prefix to all columnNames in teachers dataframe
    teachersdf = teachersdf.add_prefix('teacher')
    #Adding a new column teacherName by combining teacherfname and teacherlname
    teachersdf['teachername'] = teachersdf['teacherfname']+' '+teachersdf['teacherlname']
    print(teachersdf)
    return teachersdf

'''
This method takes 2 dataframes as input, one for student and one for teacher
Applies join logic to get the required fields for output
creates a file if doesn't exists with information on student, teacher and classes scheduled
'''
def registeredStudent(students: pandas.DataFrame, teachers: pandas.DataFrame):
    #joining both student and teacher information to create an output file
    mergeddf = pandas.merge(students, teachers, left_on = 'studentcid', right_on='teachercid')
    outputdf = mergeddf[['studentid', 'studentname', 'teacherid','teachername','studentcid']]
    #create a json file
    outputdf.to_json('data_engineer/output.json', orient='index')

def main() :
    students = processStudent()
    teachers = processTeachers()
    registeredStudent(students,teachers)

if __name__ == '__main__':
    main()

