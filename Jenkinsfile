node {
    def app

    environment {
        DOCKERHUB_CREDENTIALS = credentials('docker-hub-credentials')
    }

    stage('Initialize'){
        def dockerHome = tool 'myDocker'
        env.PATH = "${dockerHome}/bin:${env.PATH}"
    }

    stage('Clone repository') {
        /* Let's make sure we have the repository cloned to our workspace */

        checkout scm
    }

    stage('Build image') {
        /* This builds the actual image; synonymous to
        * docker build on the command line */
        app = docker.build("sofraserv/streamingquartdocker:${env.BUILD_NUMBER}")
    }

    stage('Push image') {
        docker.withRegistry('https://registry.hub.docker.com', 'docker-hub-credentials') {
            /*app.push("0.0.${env.BUILD_NUMBER}")
            *app.push("latest")*/
            app.push("test")
        }
    }
}