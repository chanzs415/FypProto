import os, subprocess, shutil, signal
import sys
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QProcess, QUrl, QTextStream, Qt, QTimer, QThread, pyqtSignal, pyqtSlot
from PyQt5.QtWidgets import QTextEdit, QApplication, QMainWindow, QStackedWidget, QFileDialog,QWidget, QVBoxLayout, QGridLayout, QPushButton, QLabel, QListWidget, QListWidgetItem, QMessageBox
from PyQt5.QtWebEngineWidgets import *
from PyQt5.QtGui import QDesktopServices,QFont
from send2trash import send2trash
from subprocess import check_output
import tkinter as tk
from tkinter import messagebox


class ListBoxWidget(QListWidget):
    dropEventSignal = QtCore.pyqtSignal() # Custom signal
    def __init__(self, parent=None):   
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.resize(600, 600)
        container_id = "sparkcontainer"  # Replace with your container name or ID
        directory = r"/app/data"
        command = f"docker exec {container_id} ls {directory}"
        try:
            output = check_output(command, shell=True).decode()
            files = output.split("\n")
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(directory, file)
                    item = QListWidgetItem(file_path)
                    item = item.text().replace('\\', '/')
                    self.addItem(item)
        except Exception as e:
            print(f"Error listing files: {e}")

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls:
            event.accept()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.setDropAction(Qt.CopyAction)
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            event.setDropAction(Qt.CopyAction)
            event.accept()

            links = []
            for url in event.mimeData().urls():
                # https://doc.qt.io/qt-5/qurl.html
                if url.isLocalFile():
                    x = (str(url.toLocalFile()))
                    links.append(str(url.toLocalFile()))
                else:
                    x = (str(url.toString()))
                    links.append(str(url.toString()))
            self.addItems(links)

            x = x.replace('/', '\\')
            print(x)

            file_extension = os.path.splitext(x)[1]

            if file_extension == ".csv":
                print("Source file is a .csv file.")
            else:
                print("Source file is not a .csv file.")

            process = QProcess(None)
            command1 = "docker cp " + x + r" sparkcontainer:/app/data"
            print(command1)
            process.start(command1)
            process.waitForFinished()

        else:
            event.ignore()
        
        self.dropEventSignal.emit() # Emit the custom signal

    def fileDel (self, filePath):
        item = self.listbox.findItems(file_path, Qt.MatchExactly)
        if item:
            self.listbox.takeItem(self.listbox.row(item[0]))


#Starting page
class Page0(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)

        layout0 = QVBoxLayout()

        self.pushButton01 = QPushButton("Start")
        self.pushButton01.setFixedSize(250, 90)
        self.pushButton01.setFont(font)
        self.pushButton01.setObjectName("pushButton01")

        self.pushButton02 = QPushButton("About")
        self.pushButton02.setFixedSize(250, 90)
        self.pushButton02.setFont(font)
        self.pushButton02.setObjectName("pushButton02")

        self.pushButton03 = QPushButton("Main Page")
        self.pushButton03.setFixedSize(250, 90)
        self.pushButton03.setFont(font)
        self.pushButton03.setObjectName("pushButton03")

        self.pushButton01.clicked.connect(self.startPage)
        self.pushButton03.clicked.connect(self.goMain)

        layout0.addWidget(self.pushButton01)
        layout0.addWidget(self.pushButton02)
        layout0.addWidget(self.pushButton03)

        self.setLayout(layout0)

        #starts the docker containers
    def startPage(self):
        process = QProcess(None)
        command1 = r'docker-compose -f C:\Users\Owner\Desktop\FYP\Changes\FypApp\docker-compose.yml up'
        process.start(command1)
        process.waitForFinished()
        output = process.readAllStandardOutput().data().decode()
        print(output)
        
        #goes to main page after containers have started up
        self.stackedWidget.setCurrentIndex(1)

    #Go to main page
    def goMain(self):
        self.stackedWidget.setCurrentIndex(1)

    #prompts a confirm close before closing app, and docker-compose stop
    def closeEvent(self):
        super().closeEvent()

#MainPage
class Page1(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)
        #1------------------------------------------------------------------------------------------

        # layout and page1
        layout1 = QGridLayout()

        #go back
        self.pushButton11 = QtWidgets.QPushButton("Back")
        self.pushButton11.setFixedSize(250, 90)
        self.pushButton11.setFont(font)
        self.pushButton11.setObjectName("pushButton11")

        #run container button
        self.pushButton12 = QtWidgets.QPushButton("Historical")
        self.pushButton12.setFixedSize(250, 90)
        self.pushButton12.setFont(font)
        self.pushButton12.setObjectName("pushButton12")

        #get data button
        self.pushButton13 = QtWidgets.QPushButton("Real-Time")
        self.pushButton13.setFixedSize(250, 90)
        self.pushButton13.setFont(font)
        self.pushButton13.setObjectName("pushButton213")

        #SQL button
        self.pushButton14 = QtWidgets.QPushButton("SQL")
        self.pushButton14.setFixedSize(250, 90)
        self.pushButton14.setFont(font)
        self.pushButton14.setObjectName("pushButton14")
        
        #stop containers button
        self.pushButton15 = QtWidgets.QPushButton("Stop Containers")
        self.pushButton15.setFixedSize(250, 90)
        self.pushButton15.setFont(font)
        self.pushButton15.setObjectName("pushButton15")


        self.label1 = QLabel(self)

        #add widgets to layout1
        layout1.addWidget(self.pushButton11,0,0)
        layout1.addWidget(self.pushButton12, 0, 1) # Add the button to the grid layout1 at row 0, column 1
        layout1.addWidget(self.pushButton13, 0, 2) # Add the button to the grid layout1 at row 0, column 2
        layout1.addWidget(self.pushButton14, 1, 0) # Add the button to the grid layout1 at row 1, column 0
        layout1.addWidget(self.pushButton15, 1, 1) # Add the button to the grid layout1 at row 1, column 1
        layout1.addWidget(self.label1, 3,0)

        # add button functionality for page 1
        #self.pushButton1.clicked.connect(self.openNewWindow)
        self.pushButton11.clicked.connect(self.goStart)
        self.pushButton12.clicked.connect(self.analyseHist)
        self.pushButton13.clicked.connect(self.analyseRT)
        self.pushButton14.clicked.connect(self.doSQL)
        self.pushButton15.clicked.connect(self.stopCommand)
        #add layout1 to page1
        self.setLayout(layout1)

    #Goes to RT page
    def analyseRT(self):
        self.stackedWidget.setCurrentIndex(3)
    
    #Goes back to starting page
    def goStart(self):
        self.stackedWidget.setCurrentIndex(0)

    def doSQL(self):
        #for now go to dummy page
        self.stackedWidget.setCurrentIndex(4)
    
    def analyseHist(self):
        #goes to page 3 to view hist data
        self.stackedWidget.setCurrentIndex(2)

    #stop containers
    def stopCommand(self):
        process = QProcess(None)
        command = r'docker-compose -f C:\Users\Owner\Desktop\FYP\Changes\FypApp\docker-compose.yml stop'
        process.start(command)
        process.waitForFinished()
        output = process.readAllStandardOutput().data().decode()
        print(output)

    def closeEvent(self):
        super().closeEvent()


#Historical data page
class Page2(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)

        #self.listview1 = ListBoxWidget(self)
        #self.listview.dropEventSignal.connect(self.trxHDFS) # Connect the signal to a slot

        #2-----------------------------------------------------------------------------------------

        # layout and page2 (Hist page)
        layout2 = QGridLayout()

        #configure 2nd page
        self.listview = ListBoxWidget(self)
        self.listview.dropEventSignal.connect(self.trxHDFS) # Connect the signal to a slot
        self.labelData21 = QLabel("Hist data here")
        self.pushButton22= QPushButton("Fetch Data")
        self.pushButton23= QPushButton("Process")
        self.pushButton24= QPushButton("Analyse")
        self.pushButton25= QPushButton("Delete")
        self.pushButton26= QPushButton("Archive")
        self.backButton27 = QPushButton('Back')
        self.pushButton28 = QPushButton('Refresh', self)

        self.pushButton22.clicked.connect(self.getHist)
        #self.pushButton22.clicked.connect(lambda: print(self.getSelectedItem()))
        self.pushButton23.clicked.connect(self.processHist)
        self.pushButton24.clicked.connect(self.viewHist)
        self.pushButton25.clicked.connect(lambda: self.confirmationBox('Delete'))
        self.pushButton26.clicked.connect(lambda: self.confirmationBox('Archive'))
        self.pushButton26.clicked.connect(self.goBack)
        self.pushButton28.clicked.connect(self.refreshHist)
        self.backButton27.clicked.connect(self.goBack)

        layout2.addWidget(self.listview, 0, 0 , 4, 1) #row, columns, occupy no. rows, occupy no. colums
        layout2.addWidget(self.labelData21, 0, 1)
        layout2.addWidget(self.pushButton22, 0, 2) 
        layout2.addWidget(self.pushButton23, 1, 1)
        layout2.addWidget(self.pushButton24, 1, 2)
        layout2.addWidget(self.pushButton25, 2, 1)
        layout2.addWidget(self.pushButton26, 2, 2)
        layout2.addWidget(self.pushButton28, 3, 1)
        layout2.addWidget(self.backButton27, 6, 3)
        
        self.setLayout(layout2)

    #fetch data from API, currently is from local host
    def getHist(self):
        #Get data from API to store in container's /app/data
        #command1 = f'docker exec -it sparkcontainer python3 mainV2.py'
        #output1 = subprocess.check_output(command1, shell=True)
        #print(output1.decode())

        #docker cp C:\Users\Owner\Desktop\FYP\Changes\FypApp\Spark\mainV2.py sparkcontainer:/app  
        #docker cp C:\Users\Owner\Desktop\FYP\Changes\FypApp\Spark\processv2.py sparkcontainer:/app  
        #docker cp C:\Users\Owner\Desktop\FYP\Changes\FypApp\Spark\combined_csv2.csv sparkcontainer:/app/data/
        #docker cp namenode:/tmp/data/ C:\Users\Owner\Desktop 
        directory_path = r'C:\Users\Owner\Desktop\FYP\Test\Changes\FypApp\hadoop_namenode'
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
        else:
            print(f"Directory '{directory_path}' does not exist.")
        command1 = f'docker exec -it sparkcontainer python3 main.py'
        #command1 = f'docker cp C:\\Users\\Owner\\Desktop\\FYP\\Test\\Changes\\FypApp\\Spark\\data-clean-realtime.py sparkcontainer:/app'
        output1 = subprocess.check_output(command1, shell=True)
        print(output1.decode())

        self.trxHDFS()

    #transfers the fetched api to hdfs
    def trxHDFS(self):
        directory_path = r'C:\Users\Owner\Desktop\FYP\Test\Changes\FypApp\hadoop_namenode'
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
        else:
            print(f"Directory '{directory_path}' does not exist.")

        command2 = f'docker cp sparkcontainer:/app/ hadoop_namenode/'
        output2 = subprocess.check_output(command2, shell=True)
        print(output2.decode())
        print("Tranferring to volume")

        command3 = f'docker cp hadoop_namenode/data namenode:/tmp'
        output3 = subprocess.check_output(command3, shell=True)
        print(output3.decode())
        print("transferring to namenode")

       
        command35 = 'docker exec -it namenode hdfs dfs -test -d /data'
        output35 = subprocess.run(command35, shell=True, capture_output=True, text=True)
        if output35.returncode == 0:
            # Remove all files in the /data directory in HDFS
            command36 = 'docker exec -it namenode hdfs dfs -rm /data/*'
            subprocess.run(command36, shell=True, check=True)
                        
            # Remove the /data directory in HDFS
            command37 = 'docker exec -it namenode hdfs dfs -rmdir /data'
            subprocess.run(command37, shell=True, check=True)
        else:
            print('/data does not exist in HDFS')


        command4 = f'docker exec -it namenode hdfs dfs -put /tmp/data /data'
        output4 = subprocess.check_output(command4, shell=True)
        print(output4.decode())
        print("transferring to hdfs")

        self.refreshHist()

    #process the data
    def processHist(self):
        try:
            directory_path = r'C:\Users\Owner\Desktop\FYP\Test\Changes\FypApp\hadoop_namenode'
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
            else:
                print(f"Directory '{directory_path}' does not exist.")

            #process data
            item = self.listview.currentItem()
            full_path = item.text()
            # Extract filename from full path
            filename = os.path.basename(full_path)
            command5 = f'docker exec -it sparkcontainer spark-submit cleanHistoricalData.py hdfs://namenode:9000/data/' + filename
            # Run the command and capture the output
            output = subprocess.check_output(command5, shell=True, stderr=subprocess.STDOUT)
            #Print the output
            print(output.decode())
            #docker exec -it sparkcontainer python3 data-clean-historical.py hdfs://namenode:9000/data/concated_df.csv

        # Show a message box after the function has finished running
            messagebox.showinfo('Finished', 'Processing complete!')
        except Exception as e:
            # Show a message box with the error message if an exception occurs
            messagebox.showerror('Error', str(e))

    #open up streamlit
    def viewHist(self):
        try:
            directory_path = r'C:\Users\Owner\Desktop\FYP\Changes\FypApp\hadoop_namenode'
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
            else:
                print(f"Directory '{directory_path}' does not exist.")
            command6 = f'docker cp sparkcontainer:/usr/local/output/ hadoop_namenode/'
            check_output(command6, shell=True)
            command7 = f'docker cp hadoop_namenode/ viscontainer:/usr/local/'
            check_output(command7, shell=True)
        except subprocess.CalledProcessError as e:
            print(f'Error: {e.returncode}, Output: {e.output.decode()}')
        
        url = QUrl("http://localhost:8501")  # URL to open in the web browser
        QDesktopServices.openUrl(url)

    #refresh the listbox
    def refreshHist(self):
        self.listview.clear()  # Clear the listbox view
        container_id = "sparkcontainer" 
        directory = r"/app/data"
        command = f"docker exec {container_id} ls {directory}"
        try:
            output = check_output(command, shell=True).decode()
            files = output.split("\n")
            for file in files:
                if file.endswith('.csv'):
                    file_path = os.path.join(directory, file)
                    item = QListWidgetItem(file_path)
                    item = item.text().replace('\\', '/')
                    self.listview.addItem(item)  # Add items back to the listbox view
        except Exception as e:
            print(f"Error listing files: {e}")

    #for delete and archive
    def confirmationBox(self, text):
        x = self.listview.currentItem()
        if x:
            if text == 'Delete':
                file_path = x.text()
                # Show a confirmation dialog before deleting the file
                confirm = QMessageBox.question(self, 'Delete File', 'Are you sure you want to delete this file?',
                                                QMessageBox.Yes | QMessageBox.No)
                if confirm == QMessageBox.Yes:
                    selected_item = self.listview.currentItem()
                    if selected_item is not None:
                        file_path = selected_item.text()
                        print(file_path)
                        if file_path:
                            try:
                                self.listview.takeItem(self.listview.row(selected_item))  # Remove the item from the QListWidget
                                container_id = "sparkcontainer"
                                command = f"docker exec {container_id} rm {file_path}"  # Delete the file from the container
                                check_output(command, shell=True)
                            except Exception as e:
                                print(f"Error deleting file: {e}")
            else:
                if text == 'Archive':
                    file_path = x.text()
                    # Show a confirmation dialog before deleting the file
                    confirm = QMessageBox.question(self, 'Archive File', 'Are you sure you want to archive this file?',
                                                    QMessageBox.Yes | QMessageBox.No)
                    if confirm == QMessageBox.Yes:
                        selected_item = self.listview.currentItem()
                        if selected_item is not None:
                            file_path = selected_item.text()
                            if file_path:
                                try:
                                    self.listview.takeItem(self.listview.row(selected_item))  # Remove the item from the QListWidget
                                    container_id = "sparkcontainer"

                                    directory = QFileDialog.getExistingDirectory(self, "Select directory")
                                    # Use the selected directory for the command
                                    command1 = f'docker cp {container_id}:{file_path} {directory}'
                                    output = subprocess.check_output(command1, shell=True)
                                    print(output.decode())
                                    
                                    command = f"docker exec {container_id} rm {file_path}"  # Delete the file from the container
                                    check_output(command, shell=True)
                                except Exception as e:
                                    print(f"Error deleting file: {e}")
        else:
            QMessageBox.warning(self, 'Warning', 'No file selected.', QMessageBox.Ok)

    def closeEvent(self):
        super().closeEvent()

    def goBack(self):
       self.stackedWidget.setCurrentIndex(1)

#real-time data page
class Page3(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)
        #3------------------------------------------------------------------------------------------

        # layout and page3(RT page)
        layout3 = QGridLayout()
        #configure 3rd page
        
        #self.listview = ListBoxWidget(self)
        self.pushButton31= QPushButton("Fetch Data")
        self.labelData32 = QLabel("RT data here")
        self.pushButton33= QPushButton("Start")
        self.pushButton34= QPushButton("Stop")
        self.pushButton35= QPushButton("Process")
        self.pushButton36= QPushButton("Analyse")
        self.backButton37 = QPushButton('Back')
        self.textEdit = QTextEdit(self)
        self.textEdit.setReadOnly(True)

        #self.pushButton31.clicked.connect(lambda: print(self.getSelectedItem()))
        self.pushButton33.clicked.connect(self.realTimeStream)
        self.pushButton34.clicked.connect(self.killStream)
        self.backButton37.clicked.connect(self.goBack)
        self.pushButton35.clicked.connect(self.processRT)
        self.pushButton36.clicked.connect(self.viewRT)

        self.timer = QTimer()
        self.timer.timeout.connect(self.updateOutput)

        layout3.addWidget(self.textEdit, 0, 0 , 4, 1) #row, columns, occupy no. rows, occupy no. colums
        layout3.addWidget(self.pushButton31, 0, 1)
        layout3.addWidget(self.labelData32, 0, 2) 
        layout3.addWidget(self.pushButton33, 1, 1)
        layout3.addWidget(self.pushButton34, 1, 2)
        layout3.addWidget(self.pushButton35, 2, 1)
        layout3.addWidget(self.pushButton36, 2, 2)
        layout3.addWidget(self.backButton37, 6, 3)
        
        self.setLayout(layout3)
    
    # RT Page ===================================================================================
    def read_output(self):
        process = self.sender()
        if isinstance(process, QProcess):
            # Read the output from the process
            data = process.readAllStandardOutput()
            text_stream = QTextStream(data)
            text_stream.setCodec(QTextCodec.codecForLocale())
            line = text_stream.readLine().decode()
            # Append the output to the QTextEdit widget
            self.text_edit.append(line)
            
    #not used
    def delete(self,x):
        try:
            os.remove(x)
            print(x + ' deleted')
        except Exception as e:
             print('Error deleting file:', str(e))


    def process_finished(self):
        process = self.sender()
        if isinstance(process, QProcess):
            # Handle process finished event
            exit_code = process.exitCode()
            # Do something with the exit code, if needed
            print("Process finished with exit code:", exit_code)    
        

    def realTimeStream(self):
        try:
            command1 = "docker exec -d sparkcontainer python3 /app/data/test2.py"
            subprocess.run(command1, shell=True, check=True)
            print("Stream1 started successfully.")

            command2 = f'docker run -it --rm ubunimage python3 /app/bin/sendStream.py -h'
            output2 = subprocess.check_output(command2, shell=True)
            print(output2.decode())
            print("command 2 ran")

            command3 = f'docker exec -d sparkcontainer python3 /app/bin/processStream.py my-stream'
            self.process53 = subprocess.Popen(command3, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            print("Stream2 started successfully.")

            command4 = f'docker exec -it sparkcontainer python3 /app/bin/sendStream.py /app/data.csv my-stream'
            self.process_thread = ProcessThread(command4)
            self.process_thread.new_output.connect(self.update_output)
            self.start_process()
            print("Stream3 started successfully.")

        except subprocess.CalledProcessError as e:
            print(f"Error starting stream: {e}")

    def updateOutput(self):
        # read available data from the process
        output = self.process55.stdout.readline().decode()
        # update text edit widget with new data
        #self.textEdit.moveCursor(Qt.QTextCursor.End)
        
        if output:
            self.textEdit.append(output)
    
    def killStream(self):
        commands = [
        "docker exec sparkcontainer pgrep -f test2.py",
        "docker exec sparkcontainer pgrep -f processStream.py",
        "docker exec sparkcontainer pgrep -f sendStream.py"
        ]
        for command in commands:
            try:
                output = subprocess.check_output(command, shell=True)
                pids = output.decode().strip().split("\n")
                for pid in pids:
                    try:
                        subprocess.check_output(f"docker exec sparkcontainer kill {pid}", shell=True)
                        print(f"Process with PID {pid} killed.")
                    except subprocess.CalledProcessError as e:
                        print(f"Error killing process with PID {pid}: {e}")
            except subprocess.CalledProcessError as e:
                print(f"Error getting process ID: {e}")
        print("Stream stopped successfully.")


    def trxHDFS(self):
        directory_path = r'C:\Users\Owner\Desktop\FYP\Test\Changes\FypApp\hadoop_namenode'
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
        else:
            print(f"Directory '{directory_path}' does not exist.")

        command2 = f'docker cp sparkcontainer:/app/data.csv hadoop_namenode/'
        output2 = subprocess.check_output(command2, shell=True)
        print(output2.decode())
        print("Tranferring to volume")

        command3 = f'docker cp hadoop_namenode/data.csv namenode:/tmp'
        output3 = subprocess.check_output(command3, shell=True)
        print(output3.decode())
        print("transferring to namenode")

        command35 = 'docker exec -it namenode hdfs dfs -test -d /data'
        output35 = subprocess.run(command35, shell=True, capture_output=True, text=True)
        if output35.returncode == 0:
            # Remove all files in the /data directory in HDFS
            command36 = 'docker exec -it namenode hdfs dfs -rm /data/*'
            subprocess.run(command36, shell=True, check=True)
                        
            # Remove the /data directory in HDFS
            command37 = 'docker exec -it namenode hdfs dfs -rmdir /data'
            subprocess.run(command37, shell=True, check=True)
        else:
            print('/data does not exist in HDFS')


        command4 = f'docker exec -it namenode hdfs dfs -put /tmp/data /data'
        output4 = subprocess.check_output(command4, shell=True)
        print(output4.decode())
        print("transferring to hdfs")

        self.refreshHist()

    def processRT(self):
        try:
            directory_path = r'C:\Users\Owner\Desktop\FYP\Changes\FypApp\hadoop_namenode'
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
            else:
                print(f"Directory '{directory_path}' does not exist.")

            command5 = f'docker exec -it sparkcontainer spark-submit cleanHistoricalData.py /app/data.csv'
            # Run the command and capture the output
            output = subprocess.check_output(command5, shell=True, stderr=subprocess.STDOUT)
            #Print the output
            print(output.decode())

        # Show a message box after the function has finished running
            messagebox.showinfo('Finished', 'Processing complete!')
        except Exception as e:
            # Show a message box with the error message if an exception occurs
            messagebox.showerror('Error', str(e))

    #open up streamlit
    def viewRT(self):
        try:
            directory_path = r'C:\Users\Owner\Desktop\FYP\Test\Changes\FypApp\hadoop_namenode'
            if os.path.exists(directory_path):
                shutil.rmtree(directory_path)
            else:
                print(f"Directory '{directory_path}' does not exist.")
            command6 = f'docker cp sparkcontainer:/usr/local/output2 hadoop_namenode/'
            check_output(command6, shell=True)
            command7 = f'docker cp hadoop_namenode/ viscontainer2:/usr/local/'
            check_output(command7, shell=True)


        except subprocess.CalledProcessError as e:
            print(f'Error: {e.returncode}, Output: {e.output.decode()}')
        
        url = QUrl("http://localhost:8502")  # URL to open in the web browser
        QDesktopServices.openUrl(url)

    def closeEvent(self):
        super().closeEvent()

    def goBack(self):
       self.stackedWidget.setCurrentIndex(1)

    @pyqtSlot()
    def start_process(self):
        self.process_thread.start()

    @pyqtSlot(str)
    def update_output(self, output):
        self.textEdit.append(output)

class ProcessThread(QThread):
    new_output = pyqtSignal(str)
    error_occurred = pyqtSignal(str)

    def __init__(self, command):
        super().__init__()
        self.command = command

    def run(self):
        try:
            process = subprocess.Popen(self.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            while True:
                output = process.stdout.readline().decode()
                if not output:
                    break
                self.new_output.emit(output)
        except Exception as e:
            self.error_occurred.emit(str(e))


#SQL page
class Page4(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)

        # layout and page4(SQL page)
        layout4 = QVBoxLayout()
       
        #configure 4th page
        self.labelData = QLabel("SQL here")
        self.backButton4 = QPushButton('Back')
        self.backButton4.clicked.connect(self.goBack)
        layout4.addWidget(self.labelData)
        layout4.addWidget(self.backButton4)
        self.setLayout(layout4)
    
    def closeEvent(self):
        super().closeEvent()

    def goBack(self):
       self.stackedWidget.setCurrentIndex(1)

class Page5(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)

        layout5 = QVBoxLayout()
       
        self.listbox_view = ListBoxWidget(self)
        self.btn = QPushButton('Get Value', self)
        self.btn.setGeometry(850, 400, 200, 50)
        self.btn.clicked.connect(lambda: print(self.getSelectedItem2()))
        layout5.addWidget(self.listbox_view)
        layout5.addWidget(self.btn)
        self.setLayout(layout5)
    
    def closeEvent(self):
        super().closeEvent()

#test loading page
class Page6(QWidget):
    def __init__(self,stackedWidget):
        super().__init__()
        self.stackedWidget = stackedWidget

        font = QtGui.QFont()
        font.setPointSize(16)

        layout6 = QVBoxLayout()
        page6 = QWidget()

        self.labelStart = QLabel('Starting please wait...')
        self.labelStart.setAlignment(Qt.AlignCenter)  # set alignment to center
        font2 = QFont()
        font2.setPointSize(30)  # set font size to 30
        self.labelStart.setFont(font2)  # set font for the label
        layout6.addWidget(self.labelStart)
        page6.setLayout(layout6)
    
    def closeEvent(self):
        super().closeEvent()


class MainWindow(QMainWindow):
    def __init__(self):
        #Mainwindow
        super().__init__()
        self.setWindowTitle("Main Window")
        self.resize(800,600)

        # create stacked widget for the different pages
        self.stackedWidget = QStackedWidget()
        self.setCentralWidget(self.stackedWidget)

        font = QtGui.QFont()
        font.setPointSize(16)

        #0------------------------------------------------------------------------------------------

        self.page0 = Page0(self.stackedWidget)
        self.page1 = Page1(self.stackedWidget)
        self.page2 = Page2(self.stackedWidget)
        self.page3 = Page3(self.stackedWidget)
        self.page4 = Page4(self.stackedWidget)
        self.page5 = Page5(self.stackedWidget)
        self.page6 = Page6(self.stackedWidget)
        #self.stackedWidget.addWidget(self.page0)

        # add pages to the main stacked widget
        self.stackedWidget.addWidget(self.page0)
        self.stackedWidget.addWidget(self.page1)
        self.stackedWidget.addWidget(self.page2)
        self.stackedWidget.addWidget(self.page3)
        self.stackedWidget.addWidget(self.page4)
        self.stackedWidget.addWidget(self.page5)
        self.stackedWidget.addWidget(self.page6)
        #self.stackedWidget.setCurrentIndex(6)

    #prompts a confirm close before closing app, and docker-compose stop
    def closeEvent(self, event):
        reply = QMessageBox.question(self, 'Confirm Exit',
                                    'Are you sure you want to exit?',
                                    QMessageBox.Yes | QMessageBox.No,
                                    QMessageBox.No)

        if reply == QMessageBox.Yes:
            dialog = QMessageBox(self)
            dialog.setWindowTitle("Exiting...")
            dialog.setText("Application is closing...")
            dialog.setStandardButtons(QMessageBox.NoButton)
            dialog.setModal(True)
            dialog.show()

            process = QProcess(None)
            command = r'docker-compose -f C:\Users\Owner\Desktop\FYP\Changes\FypApp\docker-compose.yml stop'
            process.start(command)
            process.waitForFinished()

            output = process.readAllStandardOutput().data().decode()
            print(output)

            event.accept()
        else:
            event.ignore()


if __name__ == '__main__':
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec_()

#for real-time data
#docker exec -it sparkcontainer python3 /app/data/test.py
#docker run -it --rm ubunimage python3 /app/bin/sendStream.py -h
#docker exec -it sparkcontainer python3 /app/bin/processStream.py my-stream
#docker exec -it sparkcontainer python3 /app/bin/sendStream.py /app/.\data.csv my-stream