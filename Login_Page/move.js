// seleciona o primeiro elemento com a classe "ball"
var element = document.querySelector(".ball:first-child");

// define a posição inicial do elemento em X e Y
var x = 0;
var y = 0;

// define a velocidade inicial do elemento em X e Y
var speedX = 1;
var speedY = 1;

// cria um loop que é executado a cada 10 milissegundos
setInterval(function() {
    // adiciona a velocidade atual a posição atual do elemento em X e Y
    x += speedX;
    y += speedY;

    // verifica se o elemento atingiu a borda direita ou esquerda da janela do navegador
    if (x > window.innerWidth - 50 || x < 0) {
        // inverte a direção da velocidade em X
        speedX = -speedX;
    }

    // verifica se o elemento atingiu a borda inferior ou superior da janela do navegador
    if (y > window.innerHeight - 50 || y < 0) {
        // inverte a direção da velocidade em Y
        speedY = -speedY;
    }

    // atualiza a posição do elemento no DOM
    element.style.left = x + "px";
    element.style.top = y + "px";
}, 10);