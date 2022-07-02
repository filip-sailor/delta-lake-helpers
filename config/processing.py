# Databricks notebook source

MY_CONFIG = {"key": "val"}
[print(f"Imported var: {var}") for var in dir() if var.isupper()]
