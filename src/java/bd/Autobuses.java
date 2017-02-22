/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bd;

/**
 *
 * @author MarcosPortatil
 */
public class Autobuses {
    
    private String matricula, password;

    public Autobuses(String matricula, String password) {
        this.matricula = matricula;
        this.password = password;
    }

    public String getMatricula() {
        return matricula;
    }
//
//    public void setMatricula(String matricula) {
//        this.matricula = matricula;
//    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
    
    
    
}
