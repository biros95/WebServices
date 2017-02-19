/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bd;

/**
 *
 * @author Lluis_2
 */
public class Ubicaciones {

    private String matricula;
    private double latitud, longitud;
    private String data;

    public Ubicaciones(String matricula, double latitud, double longitud, String data) {
        this.matricula = matricula;
        this.latitud = latitud;
        this.longitud = longitud;
        this.data = data;
    }

    public String getMatricula() {
        return matricula;
    }


    public double getLatitud() {
        return latitud;
    }


    public double getLongitud() {
        return longitud;
    }


    public String getData() {
        return data;
    }




}
