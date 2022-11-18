# Comprimindo imagem com python
from PIL import Image
Img = Image.open('perfil.jpg')

#Salvando nova imagem comprimida
Img.save(
        'perfil_compressed.jpg',
        'JPEG',
        optimize=True,
        quality=10
)