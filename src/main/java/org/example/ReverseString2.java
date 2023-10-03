package org.example;

import java.util.Stack;

public class ReverseString2 {

    public static void reverse(char[] str) {

        Stack<Character> st = new Stack<>();
        for (char ch : str) {
            //if((ch >= 'A' && ch <='Z')||(ch >= 'a' && ch <= 'z')){
            if(Character.isAlphabetic(ch)) {

                st.push(ch);
            }
        }

        for (int i = 0; i < str.length; i++) {
            //if((str[i] >= 'A' && str[i] <= 'Z')|| (str[i] >= 'a' && str[i]<'z'))
            //{
            if (Character.isAlphabetic(str[i])) {

                str[i] = st.pop();
            }
        }
    }
    public static void main(String[] args) {

        String input = "This -,., the Cat";
        char[] character = input.toCharArray();
        ReverseString2.reverse(character);
        String str = new String(character);
        System.out.println(str);
    }
}
