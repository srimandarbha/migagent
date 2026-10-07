#!/usr/bin/env python3
import argparse, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from simulator.runner import simulate
p=argparse.ArgumentParser(); p.add_argument('scenario'); p.add_argument('--pretty',action='store_true'); a=p.parse_args(); simulate(a.scenario,a.pretty)
