from flask import Flask, request
import os
import logging
from translate import Translator
from azure.ai.translation.text import TextTranslationClient, TranslatorCredential
from azure.ai.translation.text.models import InputTextItem
from azure.core.exceptions import HttpResponseError
from azure.core.credentials import AzureKeyCredential
from googletrans import Translator as GoogleTranslator

# Configurar el registro
logging.basicConfig(filename='/tmp/translation_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Reemplaza 'YOUR_MICROSOFT_API_KEY' con la clave API de Microsoft Azure
microsoft_translator_api_key = str(os.environ.get('MICROSOFT_TRANSLATOR_API_KEY'))
# Reemplaza 'YOUR_GOOGLE_API_KEY' con la clave API de Google Cloud
google_translate_api_key = os.environ.get('GOOGLE_TRANSLATE_API_KEY')
region = "eastus"
endpoint="https://api.cognitive.microsofttranslator.com/"

print("MICROSOFT_TRANSLATOR_API_KEY:", microsoft_translator_api_key)
print("GOOGLE_TRANSLATE_API_KEY:", google_translate_api_key)
print("ENDPOINT AZURE:", endpoint)


def traducir_con_mymemory_microsoft_y_google(texto, idioma_entrada, idioma_destino):

    try:
        # Primera opción: MyMemory
        translator = Translator(from_lang=idioma_entrada, to_lang=idioma_destino)
        traduccion = translator.translate(texto)
        if traduccion and all(error not in traduccion for error in ["límite de uso", "IS AN INVALID", "TOO MANY REQUESTS","TARGET LANGUAGE", "YOU USED ALL AVAILABLE FREE TRANSLATIONS FOR TODAY", "INVALID LANGUAGE PAIR SPECIFIED"]):
            logging.info(f'MyMemory realizó la traducción: "{texto}" -> "{traduccion}"')
            return traduccion
    except Exception as e:
        logging.error(f"Error al usar MyMemory: {e}")

    try:
        # Segunda opción: Microsoft Translator
        credential = TranslatorCredential(microsoft_translator_api_key, region)
        text_translator = TextTranslationClient(endpoint=endpoint, credential=credential)
        input_text_elements = [ InputTextItem(text = texto) ]
        response = text_translator.translate(content = input_text_elements, to = [idioma_destino], from_parameter = idioma_entrada )
        traduccion = response[0].translations[0].text if response else None
        if traduccion:
            logging.info(f"Microsoft Translator realizó la traducción: {texto!r} -> {traduccion!r}")
            return traduccion
    except HttpResponseError as e:
        logging.error(f"Error al usar Microsoft Translator: {e.error.code} {e.error.message}")

    try:
        # Tercera opción: Google Translate
        translator = GoogleTranslator()
        traduccion = translator.translate(texto, src=idioma_entrada, dest=idioma_destino).text
        if traduccion:
            logging.info(f"Google Translate realizó la traducción: {texto!r} -> {traduccion!r}")
            return traduccion
    except Exception as e:
        logging.error(f"Error al usar Google Translate: {e}")

    return "Error al traducir el texto"

@app.route('/traducir', methods=['POST'])
def traducir():
    idioma_entrada = request.form['idioma_entrada']
    idioma_destino = request.form['idioma_destino']
    texto = request.form['texto']
    traduccion = traducir_con_mymemory_microsoft_y_google(texto, idioma_entrada, idioma_destino)
    return traduccion

if __name__ == '__main__':
    from waitress import serve
    serve(app, host="0.0.0.0", port=8889)
