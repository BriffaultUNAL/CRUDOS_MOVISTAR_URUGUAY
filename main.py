#!/usr/bin/python

from src.utils import *
import sys
import os

if __name__ == "__main__":

    base = Load_raw('Consolidado Asignación Mayo.xlsx')
    base.verify()
