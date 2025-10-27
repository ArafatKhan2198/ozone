#!/usr/bin/env python3
import google.generativeai as genai

# Configure with your API key
genai.configure(api_key="AIzaSyDc5sunisIgOCq_SytqE_S47ScEF1qLfVg")

print("Available models:")
for model in genai.list_models():
    if 'generateContent' in model.supported_generation_methods:
        print(f"- {model.name}")










