<template>
<div>
    Hey!
    {{ titulo }}
    <div v-for="p of posts">
        <h1> {{ p.titulo }} </h1>
        <h2> {{ p.snippet }}</h2>
        <img :src='p.foto' />
    </div>
</div>
</template>

<script>
export default {
    name: 'app',
    data() {
        return {
            titulo: 'meu Titulo',
            posts: [{
                    titulo: 'primeiro título',
                    foto: 'http://dicasboaspracachorro.com.br/wp-content/uploads/2014/06/hotel-para-cachorro.jpeg',
                    snippet: 'Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum '
                },
                {
                    titulo: 'segundo título',
                    foto: 'http://dicasboaspracachorro.com.br/wp-content/uploads/2014/06/hotel-para-cachorro.jpeg',
                    snippet: 'segundo Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum Lorem ipsum '
                }
            ]
        }
    }
}
</script>

<style>
#app {
    font-family: 'Avenir', Helvetica, Arial, sans-serif;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
    text-align: center;
    color: #2c3e50;
    margin-top: 60px;
}

h1,
h2 {
    font-weight: normal;
}

ul {
    list-style-type: none;
    padding: 0;
}

li {
    display: inline-block;
    margin: 0 10px;
}

a {
    color: #42b983;
}
</style>
