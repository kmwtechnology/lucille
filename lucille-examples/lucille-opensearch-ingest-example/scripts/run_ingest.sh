#!/bin/bash
# run this script from top level lucille-simple-csv-solr-example directory via ./scripts/run_ingest.sh

# check if user wants to edit or run existing config
if [ -f conf/opensearchEdit.conf ]; then
    read -r -p "Edit existing configuration or run program? (e/r): " choice
else
    choice="e"
fi

if [ "$choice" = "e" ]; then
  # prompt the user for input to enron dir
  read -r -p "Enter the absolute path to enron maildir: " path_to_maildir

  # prompt user for username and password for openSearch
  read -r -p "Enter the username & password for openSearch in format {username}:{password} " credentials

  # change the vfsPath setting in the configuration file and create a new config file
  sed -e "s,Path/To/Enron/MailDir,$path_to_maildir," conf/opensearch.conf > conf/opensearchEdit.conf

  # replace url setting in the new config file 
  sed -i '' "s,username:password,$credentials," conf/opensearchEdit.conf

elif [ "$choice" = "r" ]; then
    echo "Running the program with existing configuration..."
else
    echo "Invalid choice. Please enter 'e' to edit or 'r' to run."
    exit 1
fi

# run the program
java -Dconfig.file=conf/opensearchEdit.conf -cp 'target/lib/*' com.kmwllc.lucille.core.Runner