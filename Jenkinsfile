#!groovy

// Global scope required for multi-stage persistence.
def artServer = Artifactory.server "art-p-01"
def buildInfo = Artifactory.newBuildInfo()
def agentPython3Version = 'python_3.6.1'

def updateGitHubStatus(String description, String state) {
    // State must be pending, success or failure
    withCredentials([string(credentialsId: env.GITHUB_TOKEN_NAME, variable: 'GITHUB_TOKEN')]) {            
        println("Updating GitHub pipeline status")
        long_sha = sh(returnStdout: true, script: "git rev-parse HEAD").trim()
        
        sh "curl \
            -X POST \
            -x $PROXY \
            -H \"Accept: application/vnd.github.v3+json\" \
            -H \"Authorization: token ${GITHUB_TOKEN}\" \
            \"https://api.github.com/repos/${GITHUB_ORGANISATION}/${GITHUB_PROJECT_NAME}/statuses/${long_sha}\" \
            -d \'{\"state\": \"${state}\", \"context\": \"Jenkins\", \"description\": \"${description}\", \"target_url\": \"https://jen-m.ons.statistics.gov.uk/blue/organizations/jenkins/${JENKINS_AREA}%2F${JENKINS_PROJECT_NAME}/detail/${BRANCH_NAME}/${BUILD_NUMBER}/pipeline\"}\'"      
            // Ugly classic Jenkins console
            //-d \'{\"state\": \"${state}\", \"context\": \"Jenkins\", \"description\": \"${description}\", \"target_url\": \"https://jen-m.ons.statistics.gov.uk/job/${JENKINS_VIEW}/job/${JENKINS_PROJECT_NAME}/job/${BRANCH_NAME}/${BUILD_NUMBER}/console\"}\'"       
    }
}

def pushToPyPiArtifactoryRepo(String projectName, String sourceDist = 'dist/*', String artifactoryHost = 'art-p-01') {
    withCredentials([usernamePassword(credentialsId: env.ARTIFACTORY_CREDS, usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]){
        sh "curl -u ${ARTIFACTORY_USER}:\${ARTIFACTORY_PASSWORD} -T ${sourceDist} 'http://${artifactoryHost}/artifactory/${env.ARTIFACTORY_PYPI_REPO}/${projectName}/'"
    }
}

pipeline {
    libraries {
            // Useful library from ONS internal GitLab
            lib('jenkins-pipeline-shared')
        }

    // Define environment variables
    environment {
        GITHUB_PROJECT_NAME = ""
        MAIN_BRANCH = "main"
        GITHUB_ORGANISATION = ""
        GITHUB_TOKEN_NAME = ""  // GitHUB PAT, set in Jenkins Credentials
        JENKINS_VIEW = ""
        JENKINS_PROJECT_NAME = ""
        PROXY = credentials("PROXY")  // Http proxy address, set in Jenkins Credentials
        STATUS_DESCRIPTION = ""  // Label shown on GitHub status
        // Only need these if you're deploying code to Artifactory
        ARTIFACTORY_CREDS = ""
        ARTIFACTORY_PYPI_REPO = ""
    }

    // Don't use default checkout process, as we define it as a stage below
    options {
        skipDefaultCheckout true
    }

    // Agent must always be set at the top level of the pipeline
    agent any

    stages {
        // Checkout stage to fetch code from  GitLab
        stage("Checkout") {
            // We have to specify an appropriate slave for each stage
            // Choose from download, build, test, deploy
            agent { label "download.jenkins.slave" }
            steps {
                colourText("info", "Checking out code from source control.")
                checkout scm
                // Need to have the repo checked out to get Git commit SHA
                updateGitHubStatus(env.STATUS_DESCRIPTION, 'pending')
                // Stash the files that have been checked out, for use in subsequent stages
                stash name: "Checkout", useDefaultExcludes: false
            }

        }
        stage("Build") {
            agent { label "build.${agentPython3Version}" }
            steps {
                unstash name: 'Checkout'
                colourText('info', "Building package")
                sh 'pip3 install wheel==0.29.0'
                sh 'python3 setup.py build bdist_wheel'
                stash name: "Build", useDefaultExcludes: false
            }
        }
        stage("Test") {
            agent { label "test.${agentPython3Version}" }
            steps {
                unstash name: "Checkout"
                colourText('info', "Running pytest.")                
                sh 'pip3 install pytest'
                sh 'python3 setup.py install'  // Possibly more realistic than `pip install -e .`
                sh 'python3 -m pytest'
            }
        }
        stage("Deploy") {
            when { tag "v*" }  // Only run when there's a new tag that start with a v (for version)
            agent { label "deploy.jenkins.slave" }
            steps {
                unstash name: "Build"
                colourText('info', "Deploying to Artifactory")
                pushToPyPiArtifactoryRepo("${buildInfo.name}")
            }
        }
    }
    post {
        // Update commit status with success of failure of pipeline
        success {
            unstash name: 'Checkout'
            updateGitHubStatus(env.STATUS_DESCRIPTION, 'success')
        }
        failure {
            unstash name: 'Checkout'
            updateGitHubStatus(env.STATUS_DESCRIPTION, 'failure')
        }
    }

}
