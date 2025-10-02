FROM nginx:alpine

# Копируем HTML файлы
COPY . /usr/share/nginx/html/

# Копируем конфиг nginx
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]