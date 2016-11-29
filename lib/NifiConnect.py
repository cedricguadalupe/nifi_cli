
import subprocess
import urllib
import datetime
import json
import os
import re


def add_log(level, msg):
    now_datetime = datetime.datetime.now()
    if level != "DEBUG":
        print("%s|%s|%s" % (now_datetime.strftime("%Y-%m-%d %H:%M:%S"), level, msg))
    else:
        if "DEBUG" in os.environ:
            if os.environ["DEBUG"] == "1":
                print("%s|%s|%s" % (now_datetime.strftime("%Y-%m-%d %H:%M:%S"), level, msg))



class NifiAuth:
    NONE = 0
    NORMAL = 1
    KERBEROS = 2


class NifiConnect:
    def __init__(self):
        self.url = ""
        self.auth_mode = NifiAuth.NONE
        self.username = ""
        self.password = ""
        self.token = None
        self.keytab = ""
        self.principal = ""
        pass

    def load_properties(self, env, config_parser):
        """

        :param env:
        :type env: str
        :param config_parser:
        :type config_parser: RawConfigParser
        :return:
        """
        self.url = config_parser.get(env, "nifi.url")
        if config_parser.get(env, "nifi.auth") is not None:
            if str(config_parser.get(env, "nifi.auth")).lower() == "none":
                self.auth_mode = NifiAuth.NONE
            elif str(config_parser.get(env, "nifi.auth")).lower() == "normal":
                self.auth_mode = NifiAuth.NORMAL
                self.username = config_parser.get(env, "nifi.auth.username")
                self.password = config_parser.get(env, "nifi.auth.password")
            elif str(config_parser.get(env, "nifi.auth")).lower() == "kerberos":
                self.auth_mode = NifiAuth.KERBEROS
                self.keytab = config_parser.get(env, "nifi.auth.keytab")
                self.principal = config_parser.get(env, "nifi.auth.principal")
        pass

    def connect(self):
        if self.auth_mode == NifiAuth.NONE:
            return True
        elif self.auth_mode == NifiAuth.NORMAL:
            url_to_connect = self.url + "/access/token"
            header = urllib.urlencode({
                "username": self.username,
                "password": self.password
            })
            p = subprocess.Popen("curl --data \"" + header + "\" -k " + url_to_connect, shell=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = p.communicate()
            if p.returncode == 0:
                self.token = out
                add_log("DEBUG", "Connecte au serveur Nifi Api : " + self.url)
                return True
            else:
                add_log("ERROR", "Erreur lors de la connexion au serveur Nifi Api : " + self.url)
                add_log("ERROR", err)
                add_log("ERROR", out)
                return False
        elif self.auth_mode == NifiAuth.KERBEROS:
            # TODO
            return False

    def get_curl_command(self, sub_command, other=""):
        if self.auth_mode != NifiAuth.NONE:
            return "curl -k " + other + " " + self.url + sub_command + " -H 'Authorization: Bearer " + self.token + "'"
        else:
            return "curl -k " + other + " " + self.url + sub_command

    def list_process_groups(self, id):
        curl_command = self.get_curl_command("/flow/process-groups/" + id)
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            j_response = json.loads(out)
            list_process_group = []
            for process_group in j_response["processGroupFlow"]["flow"]["processGroups"]:
                list_process_group.append([str(process_group["id"]), str(process_group["status"]["name"]),
                                           self.get_status(process_group)])
            return list_process_group
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def delete(self, id):
        info_process_group = self.info_process_group(id)
        headers = urllib.urlencode({

            "version": info_process_group["revision"]["version"]

        })
        curl_command = self.get_curl_command("/process-groups/" + id + "?" + headers, "-H 'Accept: application/json' "
                                                                                      "-X DELETE")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            try:
                json.loads(out)
                return True
            except:
                add_log("ERROR", out)
                return False
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def info(self, id):
        curl_command = self.get_curl_command("/flow/process-groups/" + id + "/status")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            return json.loads(out)
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def start(self, id):
        headers = json.dumps({
            "id": id,
            "state": "RUNNING"
        })
        curl_command = self.get_curl_command("/flow/process-groups/" + id,
                                             "-H \"Content-Type: application/json\" -H 'Accept: application/json'"
                                             " -X PUT --data '" + headers + "'")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            return True
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def stop(self, id):
        headers = json.dumps({
            "id": id,
            "state": "STOPPED"
        })
        curl_command = self.get_curl_command("/flow/process-groups/" + id,
                                             "-H \"Content-Type: application/json\" -H 'Accept: application/json'"
                                             " -X PUT --data '" + headers + "'")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            return True
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def list_templates(self):
        curl_command = self.get_curl_command("/flow/templates")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            j_response = json.loads(out)
            results = []
            for template in j_response["templates"]:
                results.append([template["id"], template["template"]["name"], template["template"]["groupId"]])
            return results
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def download(self, id):
        results = self.list_templates()
        name = ""
        for result in results:
            if result[0] == id:
                name = result[1]
        if name == "":
            raise RuntimeError("Template not found : " + id)
        directory = os.getcwd()
        regex = re.compile("[^A-Za-z0-9]")
        filename = directory + "/" + regex.sub("_", name) + ".xml"
        curl_command = self.get_curl_command("/templates/" + id + "/download")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            if os.path.exists(filename):
                os.remove(filename)
            file_res = open(filename, 'wb')
            file_res.write(out)
            file_res.close()
            print("Template downloaded : " + filename)
            return True
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def info_process_group(self, id):
        curl_command = self.get_curl_command("/process-groups/" + id)
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            return json.loads(out)
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def create_snippet(self, id):
        results = self.info_process_group(id)
        headers = json.dumps({
            "snippet": {
                "parentGroupId": results["component"]["parentGroupId"],
                "processors": {},
                "funnels": {},
                "inputPorts": {},
                "outputPorts": {},
                "remoteProcessGroups":{},
                "processGroups": {
                    id: {
                        "clientId": "nifi_cli",
                        "version": results["revision"]["version"]
                    }
                },
                "connections": {},
                "labels": {}
            }
        })
        curl_command = self.get_curl_command("/snippets", "-H \"Content-Type: application/json\" -H 'Accept: application/json' "
                                                          "-X POST --data '" + headers + "'")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            return json.loads(out)
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def download_by_process_group(self, id):
        results = self.list_templates()
        for result in results:
            if result[2] == id:
                self.delete_template(result[0])
        results = self.info(id)

        snippet = self.create_snippet(id)
        headers = json.dumps({
            "description": "Template generated by nifi_cli.py",
            "name": results["processGroupStatus"]["name"],
            "snippetId": snippet["snippet"]["id"]
        })
        curl_command = self.get_curl_command("/process-groups/" + id + "/templates",
                                             "-H \"Content-Type: application/json\" -H 'Accept: application/json' "
                                             "-X POST --data '" + headers + "'")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            result = json.loads(out)
            return self.download(result["template"]["id"])
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def upload(self, id, template_path):
        curl_command = self.get_curl_command("/process-groups/" + id + "/templates/upload",
                                             " -X POST -F template=@" + template_path)
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            return True
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)
        pass

    def delete_template(self, id):
        curl_command = self.get_curl_command("/templates/" + id, " -X DELETE")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            return True
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    def instanciate_template(self, id, id_template):
        headers = json.dumps({
            "templateId": id_template,
            "originX": 0.0,
            "originY": 0.0
        })
        curl_command = self.get_curl_command("/process-groups/" + id + "/template-instance",
                                             "-H \"Content-Type: application/json\" "
                                             "-H 'Accept: application/json' "
                                             "-X POST --data '" + headers + "'")
        p = subprocess.Popen(curl_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            try:
                return json.loads(out)
            except:
                add_log("ERROR", out)
                raise RuntimeError("Error")
        else:
            add_log("ERROR", out)
            add_log("ERROR", err)
            raise RuntimeError("Exec error" + curl_command)

    @staticmethod
    def get_status(process_group):
        if process_group["runningCount"] > 0:
            return "RUNNING (" + str(process_group["runningCount"]) + "/" + str(process_group["runningCount"] +
                                                                                process_group["stoppedCount"]) + ")"
        else:
            return "STOPPED"
