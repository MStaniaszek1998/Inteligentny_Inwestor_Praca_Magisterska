#!/bin/bash
cd /data
yamldirs /data_collector/configs/folder_structure.yaml
python3 -u general_orchestrator.py --domain-orchestrator 0
