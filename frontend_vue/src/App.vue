<template>
<div>
    Hey!
    <div v-for="p of postagens">
        <h1 class="titulo"> {{ p.titulo }} </h1>
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
            postagens: []
        }
    },
    created() {
        // this.$http.get('https://www.dizconto.com.br/app/postagem')
        //   .then(res => res.json())
        //   .then(postagens => this.postagens = postagens, err => console.log(err));

        this.$http.get('http://www.dizconto.com.br:1337/postagem')
          .then(res => res.json())
          .then(postagens => this.postagens = postagens, err => console.log(err));
    }
}
</script>

<style>

.titulo {
  font-weight: bold;
}
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
