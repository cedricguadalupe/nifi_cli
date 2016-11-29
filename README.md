# nifi_cli

$ ./nifi_cli.py
Usage: nifi_cli.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  delete                          Supprimer un process group
  delete_template                 Supprimer un template
  download                        Telecharger un template
  download_by_processor_group_id  Telecharger un template a partir d'un
                                  process...
  info                            Recuperer les informations d'un process
                                  group
  instanciate_template            Instancier un template pour creer un
                                  process...
  list_process_groups             Lister les process groups
  list_tempates                   Lister tous les templates
  start                           Demarrer tous les composants d'un process...
  stop                            Arreter tous les composants d'un process...
  upload                          Charger un template a partir d'un fichier
