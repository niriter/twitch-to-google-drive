import streamlink
from subprocess import Popen
import multiprocessing
from time import sleep
import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import time


global convert
global save_drive
global del_on_server
global timeout

convert = True          # Convert stream blocks into mp4
save_drive = True       # Save in cloud
del_on_server = True    # Delete stream blocks after cloud saving (mp4 or mkv)
timeout = 60            # Time blocks recording (example: blocks by 60 seconds)



class WorkerStopException(Exception):
    pass

class ConvertError(Exception):
    print('Error in converting file')
    pass

class Streams():
    def __init__(self, file_name='streams.txt', timeout=60):
        self.file_name = file_name
        self.time_of_parts = timeout

    def load_tasks(self):
        try:
            self.tasks = []
            with open(self.file_name, "r") as myfile:
                tasks = myfile.readlines()
            for task in tasks:
                self.tasks.append(task.replace('\n', ''))
        except:
            print('No file or no tasks')

    def google_login(self):
        gauth = GoogleAuth()
        gauth.LoadCredentialsFile("mycreds.txt")
        if gauth.credentials is None:
            gauth.GetFlow()
            gauth.flow.params.update({'access_type': 'offline'})
            gauth.flow.params.update({'approval_prompt': 'force'})
            gauth.CommandLineAuth()
        elif gauth.access_token_expired:
            gauth.Refresh()
        else:
            gauth.Authorize()
        gauth.SaveCredentialsFile("mycreds.txt")
        self.drive = GoogleDrive(gauth)
        # gauth = GoogleAuth()
        # gauth.LocalWebserverAuth()
        # self.drive = GoogleDrive(gauth)

    def start_parsing(self):
        self.google_login()
        self.streams = {}
        print("Start_pasers")
        for indx, stream in enumerate(self.tasks):
            self.streams[indx] = {'link': stream}
            self.streams[indx]['process'] = multiprocessing.Process(target=self.watcher, args=(stream, ))
            self.streams[indx]['process'].start()

    def watcher(self, stream):
        while True:
            try:
                if not ('part') in vars():
                    part = 0
                stream_url = streamlink.streams(stream)['best'].url
                ffmpeg_process = Popen(["ffmpeg", "-i", stream_url, "-c", "copy", str(stream.strip('/').split('/')[-1] + '_part_' + str(part))+'.mkv'])
                sleep(self.time_of_parts)
                ffmpeg_process.kill()
                print('New stream part')
                try:
                    functions = multiprocessing.Process(target=self.functions_while_record, args=(str(stream.strip('/').split('/')[-1] + '_part_' + str(part))+'.mkv', stream))
                    functions.start()
                except:
                    pass
                part += 1
            except:
                print(stream + " Have not live streams yet")
                sleep(5)
                continue

    def functions_while_record(self, file, stream):
        global convert
        if convert:
            new_file = file.split('.')[0]+'.mp4'
            self.convert(file, new_file)
            os.remove(file)
        else:
            new_file = file
        global save_drive
        if save_drive:
            self.send_to_drive(new_file, stream.split('/')[-1])
        global del_on_server
        if del_on_server:
            os.remove(new_file)
        raise WorkerStopException()

    def convert(self, old_file, new_file):
        try:
            convert = Popen(["ffmpeg", "-i", old_file, "-codec", "copy", new_file])
            convert.wait()
            print("Converted stream part")
            return new_file
        except:
            raise ConvertError()

    def send_to_drive(self, file_name, parent):
        print('Started Upload File')
        if parent:
            file_list = self.drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
            parent_id = False
            for file1 in file_list:
                if file1['title'] == parent:
                    parent_id = file1['id']
            if not parent_id:
                folder = self.drive.CreateFile({'title': parent, "mimeType": "application/vnd.google-apps.folder"})
                folder.Upload()
                parent_id = folder['id']
            subparent_id = False
            for file2 in self.drive.ListFile({'q': "'"+parent_id+"' in parents and trashed=false"}).GetList():
                if file2['title'] == str(time.strftime("%Y.%m.%d")):
                    subparent_id = file2['id']
            if not subparent_id:
                folder2 = self.drive.CreateFile({'title': str(time.strftime("%Y.%m.%d")), "mimeType": "application/vnd.google-apps.folder", 'parents':[{'id':parent_id}]})
                folder2.Upload()
                subparent_id = folder2['id']
            file2 = self.drive.CreateFile({'parents': [{'id': subparent_id}]})
            file2.SetContentFile(file_name)
            file2.Upload()
        else:
            file = self.drive.CreateFile()
            file.SetContentFile(file_name)
            file.Upload()
        print('Complete Upload File')
        return True

    def exit(self):
        for stream in self.streams:
            stream['process'].terminate()
            stream['process'].join()

if __name__ == "__main__":
    streams = Streams(timeout=timeout)
    try:
        streams.load_tasks()
        streams.start_parsing()
    except KeyboardInterrupt:
        streams.exit()