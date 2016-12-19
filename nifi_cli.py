#! /usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
import os
import sys
from lib.NifiConnect import NifiConnect
from lib import tabulate
from lib import click

script_dir = os.path.dirname(os.path.realpath(__file__))


@click.group()
def cli():
    pass


@cli.command()
@click.argument("id", required=False)
@click.option("--env", default="default")
def list_process_groups(id=None, env="default"):
    """
    Lister les process groups
    """
    if id is None:
        id = "root"
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    results = nifi_connection.list_process_groups(id)
    try:
        print tabulate.tabulate(results, headers=["ID", "NAME", "STATUS"], tablefmt="fancy_grid")
    except UnicodeEncodeError:
        print "ID | NAME | STATUS"
        for result in results:
            print result[0] + " | " + result[1] + " | " + result[2]


@cli.command()
@click.argument("id")
@click.option("--env", default="default")
def info(id, env="default"):
    """
    Recuperer les informations d'un process group
    """
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    results = nifi_connection.info(id)
    print("ID = " + results["processGroupStatus"]["id"])
    print("NAME = " + results["processGroupStatus"]["name"])
    print("PROCESSORS : ")
    print("\tID PROCESSOR | NAME | TYPE | STATUS")
    nb_running = 0
    nb_stopped = 0
    for processor in results["processGroupStatus"]["aggregateSnapshot"]["processorStatusSnapshots"]:
        print("\t" + processor["processorStatusSnapshot"]["id"] + " | " + processor["processorStatusSnapshot"]["name"] +
              " | " + processor["processorStatusSnapshot"]["type"] + " | " +
              processor["processorStatusSnapshot"]["runStatus"])
        if processor["processorStatusSnapshot"]["runStatus"] == "Stopped" or \
                        processor["processorStatusSnapshot"]["runStatus"] == "Invalid":
            nb_stopped += 1
        else:
            nb_running += 1
    if nb_running > 0:
        print("STATUS = RUNNING (" + str(nb_running) + "/" + str(nb_running + nb_stopped) + ")")
    else:
        print("STATUS = STOPPED (0/" + str(nb_stopped) + ")")


@cli.command()
@click.argument("id")
@click.option("--env", default="default")
def start(id, env="default"):
    """
    Demarrer tous les composants d'un process group
    """
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    nifi_connection.start(id)
    results = nifi_connection.info(id)
    print("ID = " + results["processGroupStatus"]["id"])
    print("NAME = " + results["processGroupStatus"]["name"])
    print("PROCESSORS : ")
    print("\tID PROCESSOR | NAME | TYPE | STATUS")
    nb_running = 0
    nb_stopped = 0
    for processor in results["processGroupStatus"]["aggregateSnapshot"]["processorStatusSnapshots"]:
        print("\t" + processor["processorStatusSnapshot"]["id"] + " | " + processor["processorStatusSnapshot"]["name"] +
              " | " + processor["processorStatusSnapshot"]["type"] + " | " +
              processor["processorStatusSnapshot"]["runStatus"])
        if processor["processorStatusSnapshot"]["runStatus"] == "Stopped" or \
                        processor["processorStatusSnapshot"]["runStatus"] == "Invalid":
            nb_stopped += 1
        else:
            nb_running += 1
    if nb_running > 0:
        print("STATUS = RUNNING (" + str(nb_running) + "/" + str(nb_running + nb_stopped) + ")")
    else:
        print("STATUS = STOPPED (0/" + str(nb_stopped) + ")")


@cli.command()
@click.argument("id")
@click.option("--env", default="default")
def stop(id, env="default"):
    """
    Arreter tous les composants d'un process group
    """
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    nifi_connection.stop(id)
    results = nifi_connection.info(id)
    print("ID = " + results["processGroupStatus"]["id"])
    print("NAME = " + results["processGroupStatus"]["name"])
    print("PROCESSORS : ")
    print("\tID PROCESSOR | NAME | TYPE | STATUS")
    nb_running = 0
    nb_stopped = 0
    for processor in results["processGroupStatus"]["aggregateSnapshot"]["processorStatusSnapshots"]:
        print("\t" + processor["processorStatusSnapshot"]["id"] + " | " + processor["processorStatusSnapshot"]["name"] +
              " | " + processor["processorStatusSnapshot"]["type"] + " | " +
              processor["processorStatusSnapshot"]["runStatus"])
        if processor["processorStatusSnapshot"]["runStatus"] == "Stopped" or \
                        processor["processorStatusSnapshot"]["runStatus"] == "Invalid":
            nb_stopped += 1
        else:
            nb_running += 1
    if nb_running > 0:
        print("STATUS = RUNNING (" + str(nb_running) + "/" + str(nb_running + nb_stopped) + ")")
    else:
        print("STATUS = STOPPED (0/" + str(nb_stopped) + ")")


@cli.command()
@click.option("--env", default="default")
def list_templates(env="default"):
    """
    Lister tous les templates
    """
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    results = nifi_connection.list_templates()
    try:
        print tabulate.tabulate(results, headers=["ID TEMPLATE", "NAME", "ID PROCESSOR GROUP"], tablefmt="fancy_grid")
    except UnicodeEncodeError:
        print "ID TEMPLATE | NAME | ID PROCESSOR GROUP"
        for result in results:
            print result[0] + " | " + result[1] + " | " + result[2]


@cli.command()
@click.argument("id")
@click.option("--env", default="default")
def delete_template(id, env="default"):
    """
    Supprimer un template
    """
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    if nifi_connection.delete_template(id):
        print "Template " + id + " deleted"


@cli.command()
@click.argument("id")
@click.option("--env", default="default")
def download(id, env="default"):
    """
    Telecharger un template
    """
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    nifi_connection.download(id)


@cli.command()
@click.argument("id")
@click.option("--env", default="default")
def download_by_processor_group(id, env="default"):
    """
    Telecharger un template a partir d'un process group
    """
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties("default", config)
    nifi_connection.connect()
    nifi_connection.download_by_process_group(id)


@cli.command()
@click.argument("id")
@click.option("--env", default="default")
def delete(id, env="default"):
    """
    Supprimer un process group
    """
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    if nifi_connection.delete(id):
        print("Process group is deleted : " + id)


@cli.command()
@click.argument("template", type=click.Path())
@click.argument("id", required=False)
@click.option("--env", default="default")
def upload(id, template, env="default"):
    """
    Charger un template a partir d'un fichier
    """
    if id is None:
        id = "root"
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    if nifi_connection.upload(id, template):
        print("Template is uploaded : " + template)


@cli.command()
@click.argument("id_template")
@click.argument("id", required=False)
@click.option("--env", default="default")
def instanciate_template(id_template, id, env="default"):
    """
    Instancier un template pour creer un process group
    """
    if id is None:
        id = "root"
    config = ConfigParser.RawConfigParser()
    config.read(script_dir + "/properties/nifi.properties")
    nifi_connection = NifiConnect()
    nifi_connection.load_properties(env, config)
    nifi_connection.connect()
    results = nifi_connection.instanciate_template(id, id_template)
    print("Process group is created : " + results["flow"]["processGroups"][0]["status"]["name"] + " - " +
          results["flow"]["processGroups"][0]["status"]["id"])

if __name__ == '__main__':
    cli(sys.argv[1:])

